//! The classic `JoinGroup` path.
//!
//! A native classic group runs the 5-state machine: the request either answers
//! at once, parks until the rebalance boundary, or completes the round. An
//! upgraded consumer group serves the same RPC for a hosted classic member by
//! upserting it into the next-gen state and reconciling, so both flavours of
//! `JoinGroup` live together here.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_protocol::{
    owned::{
        consumer_protocol_subscription::ConsumerProtocolSubscription,
        join_group_request::JoinGroupRequest,
    },
    primitives::uuid::Uuid,
};
use krabka_verified::consumer_downgrade_epoch;
use tokio::sync::oneshot;

use super::{
    ActorServices, FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS, JoinResult,
    ParkedWaiters, chrono_now_ms,
    heartbeat::{member_update_records, replace_static_member},
    member_state::{MemberChange, after_member_update, update_subscription},
    pending_records::PendingRecords,
    persistence::{Recorder, flush_classic_metadata, flush_pending},
    regex_resolution::{RegexResolution, apply_regex_result, maybe_update_regular_expressions},
    waiters::{complete_classic_rebalance, drain_followers_with, fence_replaced_classic_member},
};
use crate::{
    codes,
    coordinator::unified::{
        ClientIdentity, classic_ops,
        classic_state::GroupState as ClassicGroupState,
        config::{ConsumerGroupMigrationPolicy, NextGenConfig},
        consumer_state::GroupState as ConsumerState,
        first_join_member_id,
        group::{CoordinatorGroup, GroupKind},
        migration,
        reconciler::ReconcileInput,
        regex_resolver::TopicRegexResolver,
    },
};

/// Kafka's `appendGroupMetadataErrorToResponseError`: the `JoinGroup` error
/// for a group metadata write that failed.
pub(crate) fn append_error_code(error: &crate::error::BrokerError) -> i16 {
    codes::coordinator_append_error(codes::from_broker_error(error))
}

#[allow(clippy::too_many_arguments)] // Keeps the actor message boundary explicit.
pub(super) async fn handle_classic_join_message(
    group: &mut CoordinatorGroup,
    parked: &mut ParkedWaiters,
    services: ActorServices<'_>,
    mut request: JoinGroupRequest,
    version: i16,
    client_id: &str,
    client_host: &str,
    regex_resolver: &dyn TopicRegexResolver,
    reply: oneshot::Sender<JoinResult>,
) -> bool {
    // Kafka's `JoinGroupRequest.maybeOverrideRebalanceTimeout`: v0 has no
    // rebalance timeout field, so the session timeout stands in for it. The
    // native and the hosted paths below then read the same value.
    if version == 0 {
        request.rebalance_timeout_ms = request.session_timeout_ms;
    }
    if let Some(consumer) = group.as_consumer() {
        if consumer.members.is_empty() {
            // Kafka's `classicGroupJoin` sends a join to an empty consumer
            // group down the classic path, which deletes the consumer group
            // and creates a classic one; the committed offsets stay with the
            // group id.
            if super::retention::append_tombstones(
                services.offsets_log,
                &group.group_id,
                &[],
                Some(&group.kind),
                chrono_now_ms(),
            )
            .await
            .is_err()
            {
                let _ = reply.send(JoinResult {
                    error_code: codes::COORDINATOR_NOT_AVAILABLE,
                    member_id: request.member_id,
                    ..JoinResult::default()
                });
                return true;
            }
            services
                .coordinator
                .mark_classic_after_downgrade(&group.group_id);
            *group.kind_mut() = GroupKind::Classic(
                crate::coordinator::unified::classic_state::ClassicGroup::new(
                    group.group_id.clone(),
                ),
            );
        } else if services.config.migration_policy == ConsumerGroupMigrationPolicy::Disabled {
            // `throwIfClassicMemberCannotJoinConsumerGroup`: the error
            // response of `GroupCoordinatorService.joinGroup` carries only
            // the code.
            let _ = reply.send(JoinResult {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                ..JoinResult::default()
            });
            return true;
        }
    }
    if let Some(state) = group.as_classic_mut() {
        let previous = state.clone();
        let outcome = classic_ops::handle_join(
            state,
            &mut request,
            &classic_ops::JoinContext {
                client_id,
                client_host,
                version,
                initial_rebalance_delay: services.config.classic_initial_rebalance_delay,
                max_size: services.config.classic_max_size,
                now: Instant::now(),
            },
        );
        if let Some(fenced) = outcome.fenced_member.as_deref() {
            fence_replaced_classic_member(fenced, &mut parked.joiners, &mut parked.followers);
        }
        // Kafka's `prepareRebalance` from `CompletingRebalance` answers every
        // member that waits in `SyncGroup` with `REBALANCE_IN_PROGRESS`.
        if previous.state == ClassicGroupState::CompletingRebalance
            && state.state == ClassicGroupState::PreparingRebalance
        {
            drain_followers_with(&mut parked.followers, codes::REBALANCE_IN_PROGRESS);
        }
        match outcome.action {
            classic_ops::JoinAction::Immediate(result) => {
                let _ = reply.send(result);
            }
            classic_ops::JoinAction::PersistThenReply(result) => {
                if let Err(error) = flush_classic_metadata(state, services.offsets_log).await {
                    // Kafka reverts the replacement and answers with the
                    // append error, under the unknown member id and the
                    // leader from before the join.
                    let leader = previous.leader_id.clone().unwrap_or_default();
                    *state = previous;
                    tracing::warn!(group_id = %state.group_id, %error,
                        "classic static rejoin log write failed");
                    let _ = reply.send(JoinResult {
                        error_code: append_error_code(&error),
                        generation_id: state.generation_id,
                        protocol_type: state.protocol_type.clone(),
                        protocol_name: state.protocol_name.clone(),
                        leader,
                        ..JoinResult::default()
                    });
                    return true;
                }
                let _ = reply.send(result);
            }
            classic_ops::JoinAction::Park => {
                parked.joiners.insert(request.member_id, reply);
            }
            classic_ops::JoinAction::CompleteNow => {
                parked.joiners.insert(request.member_id, reply);
                // Every member joined, so none is removed and the group stays
                // non-empty: there is nothing to persist.
                let _ =
                    complete_classic_rebalance(state, &mut parked.joiners, &mut parked.followers);
            }
        }
        return true;
    }
    if group.as_consumer().is_some() {
        return classic_join_hosted(
            group,
            services,
            HostedJoin {
                request: &request,
                version,
                client: ClientIdentity {
                    id: client_id,
                    host: client_host,
                },
                regex_resolver,
                reply,
            },
        )
        .await
        .is_ok();
    }
    let _ = reply.send(JoinResult {
        error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
        member_id: request.member_id,
        ..JoinResult::default()
    });
    true
}

/// A classic `JoinGroup` for a consumer group, with the request context that
/// [`classic_join_hosted`] needs.
struct HostedJoin<'a> {
    request: &'a JoinGroupRequest,
    version: i16,
    client: ClientIdentity<'a>,
    /// Resolves the group's regular expressions with the principal of the
    /// request, as Kafka's `maybeUpdateRegularExpressions` does with the
    /// request context of the join.
    regex_resolver: &'a dyn TopicRegexResolver,
    reply: oneshot::Sender<JoinResult>,
}

/// Kafka's `validateOnlineDowngradeWithReplacedMember`: a static member that
/// joins with the classic protocol downgrades the consumer group when the
/// member it replaces is the group's only member of the consumer protocol
/// (`allMembersUseClassicProtocolExcept`), the migration policy allows a
/// downgrade, and the group fits in a classic group.
fn downgrades_with_replaced_member(
    state: &ConsumerState,
    config: &NextGenConfig,
    replaced_is_classic: bool,
) -> bool {
    let classic_members = state.members.values().filter(|m| m.is_classic()).count();
    classic_members + 1 == state.members.len()
        && !replaced_is_classic
        && config.migration_policy.allows_downgrade()
        && state.members.len() <= config.classic_max_size
}

/// The rebalance timeout that a classic `JoinGroup` gives `member_id`:
/// Kafka's `maybeUpdateRebalanceTimeoutMs(ofSentinel(..))`, where `-1` keeps
/// the stored timeout.
fn joined_rebalance_timeout(state: &ConsumerState, member_id: &str, requested_ms: i32) -> Duration {
    u64::try_from(requested_ms).map_or_else(
        |_| {
            state.members.get(member_id).map_or(
                Duration::from_millis(FALLBACK_REBALANCE_TIMEOUT_MS),
                |member| member.rebalance_timeout,
            )
        },
        Duration::from_millis,
    )
}

/// Kafka's `toTopicPartitions(subscription.ownedPartitions(), image)`: the
/// partitions a classic member says it owns, by topic id, for the topics the
/// image holds.
fn owned_partitions(
    subscription: &ConsumerProtocolSubscription,
    image: &ReconcileInput,
) -> HashMap<Uuid, Vec<i32>> {
    let mut owned: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for topic in &subscription.owned_partitions {
        if let Some(topic_id) = image.topic_id_by_name.get(&topic.topic) {
            owned
                .entry(*topic_id)
                .or_default()
                .extend(&topic.partitions);
        }
    }
    owned
}

/// A classic `JoinGroup` that [`admit_hosted_join`] lets into a consumer
/// group.
struct HostedAdmission {
    /// The member id the join is served under, a new one for a first join.
    member_id: String,
    /// The static member that the join replaces, by member id.
    replaces: Option<String>,
    /// Whether the replacement downgrades the group to classic.
    downgrade: bool,
    /// The subscription of the first protocol's metadata.
    subscription: ConsumerProtocolSubscription,
}

/// The `JoinGroup` error that [`admit_hosted_join`] answers.
struct HostedRefusal {
    error_code: i16,
    /// Empty except for `MEMBER_ID_REQUIRED`.
    member_id: String,
}

/// The checks of Kafka's `classicGroupJoinToConsumerGroup` that run before
/// the group changes: `throwIfConsumerGroupIsFull`,
/// `throwIfClassicProtocolIsNotSupported`, the member id a dynamic member at
/// v4 or later must ask for, `getOrMaybeSubscribeStaticConsumerGroupMember`
/// and `deserializeSubscription`.
///
/// # Errors
///
/// Returns the refusal of the join. Only `MEMBER_ID_REQUIRED` carries a member
/// id; every other refusal carries only its code.
fn admit_hosted_join(
    state: &ConsumerState,
    config: &NextGenConfig,
    req: &JoinGroupRequest,
    version: i16,
) -> Result<HostedAdmission, HostedRefusal> {
    let refuse = |error_code| HostedRefusal {
        error_code,
        member_id: String::new(),
    };
    let joins_unknown = req.member_id.is_empty();
    let member_id = first_join_member_id(&req.member_id);
    // `throwIfConsumerGroupIsFull`.
    if state.members.len() >= config.max_size && !state.members.contains_key(&member_id) {
        return Err(refuse(codes::GROUP_MAX_SIZE_REACHED));
    }
    // `throwIfClassicProtocolIsNotSupported`.
    let protocol_names: HashSet<&str> = req.protocols.iter().map(|p| p.name.as_str()).collect();
    if !migration::supports_classic_protocols(state, &req.protocol_type, &protocol_names) {
        return Err(refuse(codes::INCONSISTENT_GROUP_PROTOCOL));
    }
    if classic_ops::requires_known_member_id(req, version) {
        return Err(HostedRefusal {
            error_code: codes::MEMBER_ID_REQUIRED,
            member_id,
        });
    }
    // `getOrMaybeSubscribeStaticConsumerGroupMember`.
    let static_owner = req
        .group_instance_id
        .as_deref()
        .and_then(|instance_id| state.current_member_for_instance(instance_id))
        .and_then(|owner| state.members.get(owner))
        .map(|owner| (owner.member_id.clone(), owner.is_classic()));
    let replaces = match (&req.group_instance_id, &static_owner) {
        (Some(_), Some((owner, _))) if joins_unknown => Some(owner.clone()),
        (Some(_), None) if !joins_unknown => return Err(refuse(codes::UNKNOWN_MEMBER_ID)),
        (Some(_), Some((owner, _))) if !joins_unknown && *owner != member_id => {
            return Err(refuse(codes::FENCED_INSTANCE_ID));
        }
        _ => None,
    };
    let downgrade = static_owner
        .as_ref()
        .is_some_and(|(_, classic)| downgrades_with_replaced_member(state, config, *classic));
    // `deserializeSubscription`: the first protocol's metadata, and an
    // `IllegalStateException` when it does not decode.
    let subscription = req
        .protocols
        .first()
        .and_then(|protocol| migration::decode_consumer_subscription(&protocol.metadata))
        .ok_or_else(|| refuse(codes::UNKNOWN_SERVER_ERROR))?;
    Ok(HostedAdmission {
        member_id,
        replaces,
        downgrade,
        subscription,
    })
}

/// KIP-848 live migration: serves a classic `JoinGroup` for a member of a
/// consumer group, as Kafka's `classicGroupJoinToConsumerGroup` does.
///
/// The join is refused, before anything changes ([`admit_hosted_join`]), for
/// a group at its maximum size (`GROUP_MAX_SIZE_REACHED`) and for protocols that the group's classic
/// members do not all support (`INCONSISTENT_GROUP_PROTOCOL`). A member with
/// no member id gets a new one. A dynamic member at v4 or later gets
/// `MEMBER_ID_REQUIRED` with that id, and the group does not change until the
/// member joins again with it.
///
/// A static member that joins with no member id replaces the member that
/// holds its instance id, whatever protocol either speaks, with Kafka's
/// `replaceMember` records first in the batch. One that joins with a member id
/// must be the member that holds its instance id (`UNKNOWN_MEMBER_ID`,
/// `FENCED_INSTANCE_ID`).
///
/// The member is then updated as a heartbeat updates it: the regular
/// expression update, which drops a pattern the member held and may refresh
/// the group's resolutions, the subscription metadata update that bumps the
/// group epoch, the target assignment, and the member's reconciliation
/// against the partitions its subscription says it owns.
///
/// When the static member replaces the group's last member of the consumer
/// protocol and the policy allows it, the group downgrades instead
/// (`convertToClassicGroup`): the member is reconciled against the current
/// target unless the group epoch moved past it, and the consumer group's
/// tombstones and the classic group's record follow in the same batch. The
/// classic group starts at the consumer group's epoch from before the join,
/// and prepares a rebalance when its target is stale.
///
/// It replies on `reply` with the follower `JoinResult` of
/// [`migration::build_hosted_classic_join_result`]. It returns `Err` only on a
/// log-write failure, so the actor exits, and it first replies with the same
/// failure code the heartbeat path uses.
async fn classic_join_hosted(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    hosted: HostedJoin<'_>,
) -> Result<(), crate::error::BrokerError> {
    let HostedJoin {
        request: req,
        version,
        client,
        regex_resolver,
        reply,
    } = hosted;
    let config = services.config;
    let state = group.as_consumer().expect("caller verified consumer kind");
    let HostedAdmission {
        member_id,
        replaces,
        downgrade,
        subscription,
    } = match admit_hosted_join(state, config, req, version) {
        Ok(admission) => admission,
        Err(HostedRefusal {
            error_code,
            member_id,
        }) => {
            let _ = reply.send(JoinResult {
                error_code,
                member_id,
                ..JoinResult::default()
            });
            return Ok(());
        }
    };

    let state = group
        .as_consumer_mut()
        .expect("caller verified consumer kind");
    let image = services.metadata.snapshot();
    let owned = owned_partitions(&subscription, &image);
    let replacement = replaces.map(|previous| replace_static_member(state, &previous, &member_id));
    let epoch_before = state.group_epoch;
    let before = state.members.get(&member_id).map(|member| {
        (
            member.subscribed_topic_names.clone(),
            member.subscribed_topic_regex.clone(),
        )
    });
    let topics: HashSet<String> = subscription.topics.into_iter().collect();
    let names_changed = before
        .as_ref()
        .map_or(!topics.is_empty(), |(names, _)| names != &topics);
    let old_regex = before.and_then(|(_, regex)| regex);
    let rebalance_timeout = joined_rebalance_timeout(state, &member_id, req.rebalance_timeout_ms);
    let recorder = Recorder::start(state, &[&member_id]);
    migration::upsert_classic_member(
        state,
        migration::ClassicMemberRegistration {
            member_id: member_id.clone(),
            subscription_topics: topics,
            rack_id: subscription.rack_id.filter(|rack| !rack.is_empty()),
            protocols: req
                .protocols
                .iter()
                .map(|p| (p.name.clone(), p.metadata.clone()))
                .collect(),
            client_id: client.id.to_string(),
            client_host: client.host.to_string(),
            session_timeout: Duration::from_millis(
                u64::try_from(req.session_timeout_ms.max(0)).unwrap_or(FALLBACK_SESSION_TIMEOUT_MS),
            ),
            rebalance_timeout,
            instance_id: req.group_instance_id.clone(),
        },
    );
    let now_ms = chrono_now_ms();
    let regexes = RegexResolution::of(
        config,
        regex_resolver,
        services.coordinator.regex_refresh_version(),
        now_ms,
    );
    let change = MemberChange {
        member_id: &member_id,
        old_regex,
        // A classic member subscribes by name only (`setSubscribedTopicRegex("")`).
        new_regex: None,
        names_changed,
        owned: Some(&owned),
    };
    if downgrade {
        return downgrade_joining(
            group,
            services,
            DowngradeJoin {
                change,
                regexes: &regexes,
                epoch_before,
                recorder,
                replacement,
            },
            reply,
        )
        .await;
    }
    let update = after_member_update(state, config, services.metadata, change, &regexes);
    let (pending, resolutions) =
        member_update_records(recorder, state, update, &member_id, replacement);
    let flushed = flush_pending(
        state,
        pending,
        services.offsets_log,
        services.coordinator,
        now_ms,
    )
    .await;
    // Kafka's `handleRegularExpressionsResult` writes what the resolution
    // found in a batch of its own, after the join's.
    let flushed = match (flushed, resolutions) {
        (Ok(()), Some(resolved)) => {
            let result = apply_regex_result(state, resolved, &image);
            flush_pending(
                state,
                result,
                services.offsets_log,
                services.coordinator,
                chrono_now_ms(),
            )
            .await
        }
        (flushed, _) => flushed,
    };
    if let Err(e) = flushed {
        tracing::warn!(
            group_id = %state.group_id, error = %e,
            "next-gen actor exiting after hosted classic-join log-write failure",
        );
        let _ = reply.send(JoinResult {
            error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
            member_id: req.member_id.clone(),
            ..JoinResult::default()
        });
        return Err(e);
    }
    let member = state
        .members
        .get(&member_id)
        .expect("upsert_classic_member inserted the member");
    let _ = reply.send(migration::build_hosted_classic_join_result(member));
    Ok(())
}

/// What [`classic_join_hosted`] hands the downgrade it triggers.
struct DowngradeJoin<'a> {
    change: MemberChange<'a>,
    regexes: &'a RegexResolution<'a>,
    /// The group epoch before the join, which the classic group starts at.
    epoch_before: i32,
    recorder: Recorder,
    /// The records of Kafka's `replaceMember`, first in the batch.
    replacement: Option<PendingRecords>,
}

/// The downgrade branch of Kafka's `classicGroupJoinToConsumerGroup`: the
/// regular expression and subscription metadata updates run as for any join,
/// but no target is computed. A member whose group epoch is still the target
/// epoch is reconciled against the current target. `convertToClassicGroup`
/// then tombstones the consumer group and writes the classic group in the
/// same batch, and the classic group prepares a rebalance when the target is
/// stale.
async fn downgrade_joining(
    group: &mut CoordinatorGroup,
    services: ActorServices<'_>,
    join: DowngradeJoin<'_>,
    reply: oneshot::Sender<JoinResult>,
) -> Result<(), crate::error::BrokerError> {
    let DowngradeJoin {
        change,
        regexes,
        epoch_before,
        recorder,
        replacement,
    } = join;
    let member_id = change.member_id.to_owned();
    let group_id = group.group_id.clone();
    let state = group
        .as_consumer_mut()
        .expect("caller verified consumer kind");
    let image = services.metadata.snapshot();
    let mut regex_records = Vec::new();
    // What the resolution finds is dropped: once the group is classic,
    // Kafka's `handleRegularExpressionsResult` finds no consumer group.
    let (regex_update, _) = maybe_update_regular_expressions(
        state,
        change.old_regex.as_deref(),
        None,
        regexes,
        &mut regex_records,
    );
    if let Some(member) = state.members.get_mut(&member_id) {
        member.subscribed_topic_regex = None;
    }
    let bump = change.names_changed || regex_update.regex_updated();
    let partition_metadata_tombstone = update_subscription(state, services.metadata, bump);
    let rebalance = state.target.epoch < state.group_epoch;
    if !rebalance {
        state.reconcile_member(&member_id, change.owned, bump, services.metadata);
    }
    let mut pending = recorder.finish(state, None, partition_metadata_tombstone);
    pending.resolved_regexes = regex_records;
    if let Some(replacement) = replacement {
        pending = replacement.followed_by(pending);
    }
    let mut classic = migration::convert_consumer_to_classic(state, &[], &image);
    classic.generation_id = consumer_downgrade_epoch(true, epoch_before)
        .expect("every member of a downgrading group is classic");
    let now_ms = chrono_now_ms();
    let pending = pending.followed_by(migration::downgrade_pending_records(
        state, &classic, now_ms,
    ));
    let result = migration::build_hosted_classic_join_result(
        state
            .members
            .get(&member_id)
            .expect("upsert_classic_member inserted the member"),
    );
    let appended = match pending.to_batch(&group_id, now_ms) {
        Ok(batch) => services.offsets_log.append(&group_id, batch).await,
        Err(error) => Err(error),
    };
    if let Err(error) = appended {
        tracing::warn!(%group_id, %error,
            "next-gen actor exiting after classic-join downgrade log-write failure");
        let _ = reply.send(JoinResult {
            error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
            ..JoinResult::default()
        });
        return Err(error);
    }
    services.coordinator.mark_classic_after_downgrade(&group_id);
    if rebalance {
        classic.prepare_rebalance(
            services.config.classic_initial_rebalance_delay,
            Instant::now(),
        );
    }
    *group.kind_mut() = GroupKind::Classic(classic);
    let _ = reply.send(result);
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            GroupActorMessage, GroupKindTag, SyncResult,
            test_support::{
                completing_classic_group, decode_assignment, last_classic_metadata,
                make_coordinator, make_coordinator_with_config, make_coordinator_with_topic_config,
                make_coordinator_with_topic_policy, rpc, seed_and_upgrade,
            },
        },
        classic_state::GroupState as ClassicGroupState,
        config::{ConsumerGroupMigrationPolicy as Policy, NextGenConfig},
        persistence_next_gen::NextGenKey,
        regex_resolver::FixedRegexResolver,
    };

    /// Kafka's `group.initial.rebalance.delay.ms = 0`: the first member of a
    /// new group gets its `JoinGroup` answer as soon as the round opens, rather
    /// than after the three-second default batching window.
    ///
    /// The member joins as a v4 client does: an empty member id answers
    /// `MEMBER_ID_REQUIRED` with the id to use, and the rejoin with that id opens
    /// the round.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_initial_rebalance_delay_completes_a_new_groups_first_join_at_once() {
        let (coord, _log) = make_coordinator_with_config(NextGenConfig {
            classic_initial_rebalance_delay: std::time::Duration::ZERO,
            ..NextGenConfig::assigning_at_once()
        });
        let handle =
            crate::coordinator::unified::actor::test_support::marked_classic_handle(&coord, "g");

        let assigned = rpc::classic_join(&handle, "", "t").await;
        check!(assigned.error_code == codes::MEMBER_ID_REQUIRED);
        let member_id = assigned.member_id;

        let join = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rpc::classic_join(&handle, &member_id, "t"),
        )
        .await
        .expect("a zero delay must not wait out a batching window");

        check!(join.error_code == codes::NONE);
        check!(join.generation_id == 1);
        check!(join.member_id == member_id);
        check!(join.leader == member_id);
        check!(join.protocol_name.as_deref() == Some("range"));
        let members: Vec<&str> = join.members.iter().map(|m| m.member_id.as_str()).collect();
        check!(members == [member_id.as_str()]);
    }

    /// Kafka's `JoinGroupRequest.maybeOverrideRebalanceTimeout`: a v0 request
    /// has no rebalance timeout field, so its session timeout is the member's
    /// rebalance timeout, and that is what the group persists. From v1 the
    /// request's own rebalance timeout stands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_v0_takes_the_rebalance_timeout_from_the_session_timeout() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        // (version, rebalance timeout the request carries, the one persisted)
        for (version, requested_ms, want_ms) in [(0, -1, 45_000), (1, 90_000, 90_000)] {
            let (coord, log) = make_coordinator_with_config(NextGenConfig {
                classic_initial_rebalance_delay: Duration::ZERO,
                ..NextGenConfig::assigning_at_once()
            });
            let handle = crate::coordinator::unified::actor::test_support::marked_classic_handle(
                &coord, "g",
            );
            let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 45_000,
                    rebalance_timeout_ms: requested_ms,
                    protocol_type: "consumer".into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: "range".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                version,
                client_id: "client-a".into(),
                client_host: "127.0.0.1".into(),
                regex_resolver:
                    crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
                reply: tx,
            })
            .await;
            let joined = rx.await.unwrap();
            check!(joined.error_code == codes::NONE, "v{version}");

            let synced = rpc::classic_sync(&handle, &joined.member_id, joined.generation_id).await;
            check!(synced.error_code == codes::NONE, "v{version}");

            let persisted = last_classic_metadata(&log).await;
            check!(persisted.members.len() == 1, "v{version}");
            check!(
                persisted.members[0].rebalance_timeout_ms == want_ms,
                "v{version}"
            );
        }
    }

    /// A `Stable` group whose one member, `m1`, is the static member
    /// `instance-1`, and a `JoinGroup` v4 from a restarted `instance-1`.
    fn stable_static_group_and_rejoin() -> (CoordinatorGroup, JoinGroupRequest) {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        let mut group = completing_classic_group(&["m1"]);
        let state = group.as_classic_mut().unwrap();
        let member = state.members.get_mut("m1").unwrap();
        member.group_instance_id = Some("instance-1".into());
        member.assignment = Some(Bytes::from_static(b"assignment"));
        state
            .static_members
            .insert("instance-1".into(), "m1".into());
        state.state = ClassicGroupState::Stable;
        let request = JoinGroupRequest {
            group_id: "g".into(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: String::new(),
            group_instance_id: Some("instance-1".into()),
            protocol_type: "consumer".into(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: Bytes::from_static(b"new-subscription"),
                ..Default::default()
            }],
            ..Default::default()
        };
        (group, request)
    }

    async fn send_join(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        req: JoinGroupRequest,
    ) -> JoinResult {
        send_join_at(handle, req, 4).await
    }

    async fn send_join_at(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        req: JoinGroupRequest,
        version: i16,
    ) -> JoinResult {
        let rx = rpc::begin(handle, |tx| GroupActorMessage::ClassicJoin {
            req,
            version,
            client_id: "new-client".into(),
            client_host: "new-host".into(),
            regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
            reply: tx,
        })
        .await;
        rx.await.unwrap()
    }

    /// #789: Kafka's `updateStaticMemberThenRebalanceOrCompleteJoin` in
    /// `Stable`: a new member id replaces the old one, keeps the client and
    /// the assignment, and is persisted before the reply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_stable_static_rejoin_persists_replaced_member() {
        let (coord, log) = make_coordinator();
        let (group, request) = stable_static_group_and_rejoin();
        let (handle, generation) = super::super::test_support::seed_classic_group(&coord, group);

        let response = send_join(&handle, request).await;

        check!(response.member_id.starts_with("instance-1-"));
        check!(
            response
                == JoinResult {
                    error_code: codes::NONE,
                    generation_id: generation,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    leader: "m1".into(),
                    skip_assignment: false,
                    member_id: response.member_id.clone(),
                    members: Vec::new(),
                }
        );
        let persisted = last_classic_metadata(&log).await;
        check!(persisted.generation == generation);
        check!(persisted.members.len() == 1);
        check!(persisted.members[0].member_id == response.member_id);
        check!(persisted.members[0].client_id == "client");
        check!(persisted.members[0].client_host == "host");
        check!(persisted.members[0].subscription == Bytes::from_static(b"new-subscription"));
        check!(persisted.members[0].assignment == Bytes::from_static(b"assignment"));
    }

    /// Kafka reverts the replacement when the write fails and answers with
    /// the append error under the unknown member id.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_static_rejoin_append_failure_rolls_back_and_reports_error() {
        let (coord, log) = make_coordinator();
        let (group, request) = stable_static_group_and_rejoin();
        let (handle, generation) = super::super::test_support::seed_classic_group(&coord, group);
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let response = send_join(&handle, request).await;

        check!(
            response
                == JoinResult {
                    error_code: codes::NOT_COORDINATOR,
                    generation_id: generation,
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                    leader: "m1".into(),
                    ..JoinResult::default()
                }
        );
        let view = rpc::classic_inspect(&handle).await;
        rpc::check_stable_classic_member(&view, "m1");
        check!(view.members[0].protocol_metadata == Bytes::from_static(b"subscription"));
        check!(view.members[0].assignment.as_deref() == Some(&b"assignment"[..]));
        check!(log.batches().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_classic_member_joins_upgraded_group_and_gets_assignment() {
        // `Upgrade` policy: keep the group consumer-kind after the native
        // member leaves in `seed_and_upgrade` (see the note above).
        let (coord, _log) = make_coordinator_with_topic_policy(
            "t",
            2,
            crate::coordinator::unified::config::ConsumerGroupMigrationPolicy::Upgrade,
        );
        let handle = seed_and_upgrade(&coord, "t").await;

        // First bring m-classic fully in sync so it holds a stable assignment.
        let join_c = rpc::classic_join(&handle, "m-classic", "t").await;
        let _ = rpc::classic_sync(&handle, "m-classic", join_c.generation_id).await;

        // A brand-new classic member m2 joins the already-upgraded group as a
        // follower at its member epoch.
        let join2 = rpc::classic_join(&handle, "m2", "t").await;
        let m2 = rpc::describe_member(&handle, "m2").await;
        assert!(join2 == follower_join("m2", m2.member_epoch));

        // Kafka's `classicGroupJoinToConsumerGroup` reconciles a classic
        // member only on its `JoinGroup`, so both members rejoin to pick up
        // the rebalanced two-way split, m-classic first to release the
        // partition m2 takes, and each syncs at the generation it was given.
        let rejoin_c = rpc::classic_join(&handle, "m-classic", "t").await;
        let sync_c = rpc::classic_sync(&handle, "m-classic", rejoin_c.generation_id).await;
        let rejoin2 = rpc::classic_join(&handle, "m2", "t").await;
        let sync2 = rpc::classic_sync(&handle, "m2", rejoin2.generation_id).await;
        assert!(sync_c.error_code == codes::NONE);
        assert!(sync2.error_code == codes::NONE);

        // Collect each member's partitions of "t".
        let parts = |s: &SyncResult| -> Vec<i32> {
            decode_assignment(&s.assignment)
                .assigned_partitions
                .iter()
                .find(|tp| tp.topic == "t")
                .map(|tp| tp.partitions.clone())
                .unwrap_or_default()
        };
        let p_c = parts(&sync_c);
        let p_2 = parts(&sync2);
        assert!(!p_2.is_empty(), "the new member must receive an assignment");

        // Disjoint, and together cover {0, 1}.
        let set_c: std::collections::HashSet<i32> = p_c.iter().copied().collect();
        let set_2: std::collections::HashSet<i32> = p_2.iter().copied().collect();
        assert!(
            set_c.is_disjoint(&set_2),
            "the two members must hold disjoint partitions"
        );
        let mut union: Vec<i32> = set_c.union(&set_2).copied().collect();
        union.sort_unstable();
        assert!(
            union == vec![0, 1],
            "the union of partitions must be {{0, 1}}"
        );
    }

    /// Kafka's `classicGroupJoin` against a consumer group: (label, policy,
    /// native members) to (join error code, consumer group tombstoned, group
    /// classic afterwards). An empty consumer group is replaced by a classic
    /// group; a live one under policy `disabled` refuses the classic member
    /// with `INCONSISTENT_GROUP_PROTOCOL`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_join_against_a_consumer_group_follows_kafka() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, config::ConsumerGroupMigrationPolicy as Policy,
        };

        let rows = [
            (
                "empty consumer group",
                Policy::Disabled,
                0,
                (codes::MEMBER_ID_REQUIRED, true, true),
            ),
            (
                "live consumer group, policy disabled",
                Policy::Disabled,
                1,
                (codes::INCONSISTENT_GROUP_PROTOCOL, false, false),
            ),
            (
                "live consumer group, policy bidirectional",
                Policy::Bidirectional,
                1,
                (codes::MEMBER_ID_REQUIRED, false, false),
            ),
        ];
        for (label, policy, members, want) in rows {
            let (coord, log) = make_coordinator_with_topic_policy("t", 1, policy);
            let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
            for index in 0..members {
                let joined =
                    rpc::consumer_heartbeat(&handle, &format!("native-{index}"), 0, Some("t"))
                        .await;
                assert!(joined.error_code == codes::NONE, "{label}");
            }

            let joined = rpc::classic_join(&handle, "", "t").await;

            let rx = rpc::begin(&handle, |tx| GroupActorMessage::ClassicInspect {
                reply: tx,
            })
            .await;
            let is_classic = rx.await.is_ok();
            let got = (
                joined.error_code,
                log.has_next_gen_group_metadata_tombstone("g").await,
                is_classic,
            );
            check!(got == want, "{label}");
        }
    }

    /// Kafka's `classicGroupJoinToConsumerGroup` answer: no leader and no
    /// member list, so the client follows, and the member epoch as the
    /// generation.
    fn follower_join(member_id: &str, member_epoch: i32) -> JoinResult {
        JoinResult {
            error_code: codes::NONE,
            generation_id: member_epoch,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            member_id: member_id.into(),
            ..JoinResult::default()
        }
    }

    /// A `JoinGroup` for topic `t` as a classic consumer client sends it.
    async fn join_at(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        member_id: &str,
        group_instance_id: Option<&str>,
        version: i16,
    ) -> JoinResult {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        send_join_at(
            handle,
            JoinGroupRequest {
                group_id: "g".into(),
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 300_000,
                member_id: member_id.into(),
                group_instance_id: group_instance_id.map(Into::into),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: crate::coordinator::unified::actor::test_support::subscription_blob(
                        &["t"],
                    ),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version,
        )
        .await
    }

    /// A classic consumer that a rolling downgrade restarts joins a consumer
    /// group that still has a native member, as Kafka's
    /// `classicGroupJoinToConsumerGroup` serves it.
    ///
    /// A dynamic member at v4 or later gets `MEMBER_ID_REQUIRED` and a new id,
    /// and the group does not change. A member at v3 and a static member join
    /// at once with a new id. Every member then joins as a follower: the
    /// result has no leader and no member list, so the client sends an empty
    /// `SyncGroup` and does not run its assignor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_classic_member_joins_a_consumer_group_as_a_follower() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, config::ConsumerGroupMigrationPolicy as Policy,
        };

        // (label, version, group instance id, MEMBER_ID_REQUIRED first)
        let rows = [
            ("dynamic member at v9", 9, None, true),
            ("dynamic member at v4", 4, None, true),
            ("dynamic member at v3", 3, None, false),
            ("static member at v9", 9, Some("instance-1"), false),
        ];
        for (label, version, instance_id, member_id_required) in rows {
            let (coord, _log) = make_coordinator_with_topic_policy("t", 2, Policy::Bidirectional);
            let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
            let native = rpc::consumer_heartbeat(&handle, "native", 0, Some("t")).await;
            assert!(native.error_code == codes::NONE, "{label}");

            let mut joined = join_at(&handle, "", instance_id, version).await;
            if member_id_required {
                check!(!joined.member_id.is_empty(), "{label}");
                check!(
                    joined
                        == JoinResult {
                            error_code: codes::MEMBER_ID_REQUIRED,
                            member_id: joined.member_id.clone(),
                            ..JoinResult::default()
                        },
                    "{label}"
                );
                let rx = rpc::begin(&handle, |tx| GroupActorMessage::Describe { reply: tx }).await;
                let members: Vec<String> = rx
                    .await
                    .unwrap()
                    .members
                    .into_iter()
                    .map(|member| member.member_id)
                    .collect();
                check!(members == ["native"], "{label}");
                joined = join_at(&handle, &joined.member_id, instance_id, version).await;
            }

            check!(!joined.member_id.is_empty(), "{label}");
            let member = rpc::describe_member(&handle, &joined.member_id).await;
            check!(member.is_classic, "{label}");
            check!(
                joined == follower_join(&member.member_id, member.member_epoch),
                "{label}"
            );
        }
    }

    /// The generation that a hosted member's `JoinGroup` gives is its member
    /// epoch, which its `OffsetCommit` passes the fence with, also after other
    /// members moved the group epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hosted_member_commits_at_the_generation_its_join_gives() {
        use crate::coordinator::unified::{
            actor::{CommitFence, GroupKindTag},
            config::ConsumerGroupMigrationPolicy as Policy,
        };

        let (coord, _log) = make_coordinator_with_topic_policy("t", 2, Policy::Bidirectional);
        let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
        let native = rpc::consumer_heartbeat(&handle, "native-1", 0, Some("t")).await;
        assert!(native.error_code == codes::NONE);
        let joined = rpc::classic_join(&handle, "m-classic", "t").await;
        assert!(joined.error_code == codes::NONE);
        let synced = rpc::classic_sync(&handle, "m-classic", joined.generation_id).await;
        assert!(synced.error_code == codes::NONE);

        // A second native member moves the group epoch, and the classic member
        // joins again with the same subscription.
        let second = rpc::consumer_heartbeat(&handle, "native-2", 0, Some("t")).await;
        assert!(second.error_code == codes::NONE);
        let rejoined = rpc::classic_join(&handle, "m-classic", "t").await;
        let member = rpc::describe_member(&handle, "m-classic").await;

        check!(rejoined == follower_join("m-classic", member.member_epoch));
        check!(
            rpc::validate_commit(
                &handle,
                "m-classic",
                rejoined.generation_id,
                CommitFence::Offset { api_version: 9 },
                &[],
            )
            .await
                == Ok(())
        );
    }

    /// The keys of `batch`, each with whether its record is a tombstone, and
    /// `None` for a key outside the consumer-group family.
    fn batch_shape(
        batch: &krabka_protocol::records::RecordBatch,
    ) -> Vec<(Option<NextGenKey>, bool)> {
        use crate::coordinator::unified::persistence::{Key, parse_key};
        batch
            .records
            .iter()
            .map(|record| {
                let key = record.key.as_ref().and_then(|key| match parse_key(key) {
                    Ok(Key::NextGen(key)) => Some(key),
                    _ => None,
                });
                (key, record.value.is_none())
            })
            .collect()
    }

    #[derive(Clone, Copy)]
    struct PersistedBatchCount(usize);

    /// Exactly one new durable join batch, before the caller checks its scenario-specific keys.
    async fn new_join_batch_shape(
        log: &crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog,
        before: PersistedBatchCount,
    ) -> Vec<(Option<NextGenKey>, bool)> {
        let batches = log.batches().await;
        check!(batches.len() == before.0 + 1);
        batch_shape(batches.last().expect("the join's batch"))
    }

    /// The records of Kafka's `replaceMember` for `old` replaced by `new`:
    /// the old member's tombstones, then the new member's subscription,
    /// target and current assignment.
    fn replace_member_shape(old: &str, new: &str) -> Vec<(Option<NextGenKey>, bool)> {
        let group_id = || "g".to_string();
        vec![
            (
                Some(NextGenKey::CurrentMemberAssignment {
                    group_id: group_id(),
                    member_id: old.into(),
                }),
                true,
            ),
            (
                Some(NextGenKey::TargetAssignmentMember {
                    group_id: group_id(),
                    member_id: old.into(),
                }),
                true,
            ),
            (
                Some(NextGenKey::MemberMetadata {
                    group_id: group_id(),
                    member_id: old.into(),
                }),
                true,
            ),
            (
                Some(NextGenKey::MemberMetadata {
                    group_id: group_id(),
                    member_id: new.into(),
                }),
                false,
            ),
            (
                Some(NextGenKey::TargetAssignmentMember {
                    group_id: group_id(),
                    member_id: new.into(),
                }),
                false,
            ),
            (
                Some(NextGenKey::CurrentMemberAssignment {
                    group_id: group_id(),
                    member_id: new.into(),
                }),
                false,
            ),
        ]
    }

    /// A native consumer member `member_id` that joins subscribed to `t`, as
    /// the static member `instance_id` when one is given.
    async fn native_join(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
        member_id: &str,
        instance_id: Option<&str>,
    ) {
        let joined = rpc::consumer_request(
            handle,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch: 0,
                instance_id: instance_id.map(Into::into),
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
        )
        .await;
        assert!(joined.error_code == codes::NONE);
    }

    /// The member ids `Describe` reports, sorted.
    async fn member_ids(
        handle: &crate::coordinator::unified::actor::GroupActorHandle,
    ) -> Vec<String> {
        let mut ids: Vec<String> = rpc::describe(handle)
            .await
            .members
            .into_iter()
            .map(|member| member.member_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Kafka's `getOrMaybeSubscribeStaticConsumerGroupMember` for a classic
    /// join with no member id: the new member takes the place of the classic
    /// member that holds its instance id, with its target and assignment, and
    /// Kafka's `replaceMember` records open the batch. A group that keeps a
    /// member of the consumer protocol stays a consumer group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_static_classic_member_replaces_the_member_that_holds_its_instance_id() {
        let (coord, log) = make_coordinator_with_topic_policy("t", 2, Policy::Bidirectional);
        let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
        native_join(&handle, "native", None).await;
        let first = join_at(&handle, "", Some("instance-1"), 9).await;
        assert!(first.error_code == codes::NONE);
        let held = rpc::describe_member(&handle, &first.member_id).await;
        let batches_before = log.batches().await.len();

        let second = join_at(&handle, "", Some("instance-1"), 9).await;

        check!(second.member_id != first.member_id);
        let replaced = rpc::describe_member(&handle, &second.member_id).await;
        check!(second == follower_join(&second.member_id, replaced.member_epoch));
        check!(replaced.is_classic);
        check!(replaced.instance_id.as_deref() == Some("instance-1"));
        check!(replaced.target_partitions == held.target_partitions);
        let mut want_members = vec!["native".to_string(), second.member_id.clone()];
        want_members.sort_unstable();
        check!(member_ids(&handle).await == want_members);
        let shape = new_join_batch_shape(&log, PersistedBatchCount(batches_before)).await;
        check!(shape[..6] == replace_member_shape(&first.member_id, &second.member_id)[..]);
    }

    /// A static classic member that joins with a member id must be the member
    /// that holds its instance id: (label, member id, where `None` is the id
    /// the static member `instance-1` was given, instance id) to Kafka's
    /// error, which carries only the code, and the group does not change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_static_classic_rejoin_with_a_member_id_is_checked_as_kafka_does() {
        let rows = [
            (
                "an instance id no member holds",
                None,
                "instance-2",
                codes::UNKNOWN_MEMBER_ID,
            ),
            (
                "an instance id another member holds",
                Some("other"),
                "instance-1",
                codes::FENCED_INSTANCE_ID,
            ),
        ];
        for (label, member_id, instance_id, want) in rows {
            let (coord, log) = make_coordinator_with_topic_policy("t", 2, Policy::Bidirectional);
            let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
            native_join(&handle, "native", None).await;
            let joined = join_at(&handle, "", Some("instance-1"), 9).await;
            assert!(joined.error_code == codes::NONE, "{label}");
            let batches_before = log.batches().await.len();

            let member_id = member_id.unwrap_or(&joined.member_id);
            let refused = join_at(&handle, member_id, Some(instance_id), 9).await;

            check!(
                refused
                    == JoinResult {
                        error_code: want,
                        ..JoinResult::default()
                    },
                "{label}"
            );
            check!(log.batches().await.len() == batches_before, "{label}");
            let mut want_members = vec![joined.member_id.clone(), "native".to_string()];
            want_members.sort_unstable();
            check!(member_ids(&handle).await == want_members, "{label}");
        }
    }

    /// Kafka's `throwIfConsumerGroupIsFull` and
    /// `throwIfClassicProtocolIsNotSupported`, which refuse a classic join to
    /// a consumer group before anything else: (label, max size, protocol
    /// type, protocol name) to the error, which carries only the code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_classic_join_that_the_group_cannot_take_is_refused() {
        use krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol;

        let rows = [
            (
                "a full group",
                2,
                "consumer",
                "range",
                codes::GROUP_MAX_SIZE_REACHED,
            ),
            (
                "another protocol type",
                10,
                "connect",
                "range",
                codes::INCONSISTENT_GROUP_PROTOCOL,
            ),
            (
                "a protocol the classic member lacks",
                10,
                "consumer",
                "roundrobin",
                codes::INCONSISTENT_GROUP_PROTOCOL,
            ),
        ];
        for (label, max_size, protocol_type, protocol_name, want) in rows {
            let (coord, _log) = make_coordinator_with_topic_config(
                "t",
                2,
                NextGenConfig {
                    migration_policy: Policy::Bidirectional,
                    max_size,
                    ..NextGenConfig::assigning_at_once()
                },
            );
            let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
            native_join(&handle, "native", None).await;
            assert!(
                rpc::classic_join(&handle, "m-classic", "t")
                    .await
                    .error_code
                    == codes::NONE,
                "{label}"
            );

            let refused = send_join_at(
                &handle,
                JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 45_000,
                    rebalance_timeout_ms: 300_000,
                    member_id: "m-new".into(),
                    protocol_type: protocol_type.into(),
                    protocols: vec![JoinGroupRequestProtocol {
                        name: protocol_name.into(),
                        metadata:
                            crate::coordinator::unified::actor::test_support::subscription_blob(&[
                                "t",
                            ]),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                9,
            )
            .await;

            check!(
                refused
                    == JoinResult {
                        error_code: want,
                        ..JoinResult::default()
                    },
                "{label}"
            );
            check!(
                member_ids(&handle).await == ["m-classic", "native"],
                "{label}"
            );
        }
    }

    /// Kafka's `validateOnlineDowngradeWithReplacedMember`: a static classic
    /// member that replaces the group's last member of the consumer protocol
    /// downgrades the group in the join's own batch. The replacement records
    /// open it, the consumer group's tombstones follow, and the classic
    /// group's record closes it. The target is current, so the classic group
    /// is stable at the consumer group's epoch, holding every member's
    /// target.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_static_classic_member_that_replaces_the_last_native_member_downgrades() {
        let (coord, log) = make_coordinator_with_topic_policy("t", 2, Policy::Bidirectional);
        let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
        native_join(&handle, "native", Some("instance-1")).await;
        assert!(
            rpc::classic_join(&handle, "m-classic", "t")
                .await
                .error_code
                == codes::NONE
        );
        let epoch = rpc::describe(&handle).await.group_epoch;
        let batches_before = log.batches().await.len();

        let joined = join_at(&handle, "", Some("instance-1"), 9).await;

        check!(joined == follower_join(&joined.member_id, epoch));
        let view = rpc::classic_inspect(&handle).await;
        let mut members: Vec<(String, Option<String>)> = view
            .members
            .iter()
            .map(|member| (member.member_id.clone(), member.group_instance_id.clone()))
            .collect();
        members.sort_unstable();
        let mut want = vec![
            ("m-classic".to_string(), None),
            (joined.member_id.clone(), Some("instance-1".to_string())),
        ];
        want.sort_unstable();
        check!(
            (view.state, view.generation_id, members) == (ClassicGroupState::Stable, epoch, want)
        );
        let shape = new_join_batch_shape(&log, PersistedBatchCount(batches_before)).await;
        check!(shape[..6] == replace_member_shape("native", &joined.member_id)[..]);
        check!(
            shape[shape.len() - 3..]
                == [
                    (
                        Some(NextGenKey::PartitionMetadata {
                            group_id: "g".into()
                        }),
                        true
                    ),
                    (
                        Some(NextGenKey::GroupMetadata {
                            group_id: "g".into()
                        }),
                        true
                    ),
                    (None, false),
                ]
        );
    }

    fn fixed_topic_resolver() -> std::sync::Arc<FixedRegexResolver> {
        std::sync::Arc::new(FixedRegexResolver::new(&[("t.*", &["t"])]))
    }

    /// Kafka's `classicGroupJoinToConsumerGroup` runs
    /// `maybeUpdateRegularExpressions` with the request context of the join:
    /// a classic member's join refreshes the stale resolutions of a group
    /// whose other members subscribe to a pattern, with its own principal's
    /// resolver.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_classic_join_refreshes_the_groups_stale_regular_expressions() {
        let (coord, _log) = make_coordinator_with_topic_config(
            "t",
            2,
            NextGenConfig {
                migration_policy: Policy::Bidirectional,
                regex_refresh_interval: Duration::ZERO,
                regex_refresh_min_interval: Duration::ZERO,
                ..NextGenConfig::assigning_at_once()
            },
        );
        let handle = coord.get_or_create_group("g", GroupKindTag::Consumer);
        let native_resolver = fixed_topic_resolver();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Heartbeat {
                request: ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "native".into(),
                    member_epoch: 0,
                    subscribed_topic_regex: Some("t.*".into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                client_id: "client-a".into(),
                client_host: String::new(),
                regex_resolver: native_resolver.clone(),
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap().error_code == codes::NONE);
        assert!(native_resolver.calls() == 1);
        let classic_resolver = fixed_topic_resolver();

        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicJoin {
                req: JoinGroupRequest {
                    group_id: "g".into(),
                    session_timeout_ms: 45_000,
                    rebalance_timeout_ms: 300_000,
                    member_id: "m-classic".into(),
                    protocol_type: "consumer".into(),
                    protocols: vec![
                        krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                            name: "range".into(),
                            metadata:
                                crate::coordinator::unified::actor::test_support::subscription_blob(
                                    &["t"],
                                ),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                version: 9,
                client_id: "client-b".into(),
                client_host: "127.0.0.1".into(),
                regex_resolver: classic_resolver.clone(),
                reply: tx,
            })
            .await
            .unwrap();
        let joined = rx.await.unwrap();

        check!(joined.error_code == codes::NONE);
        check!(classic_resolver.calls() == 1);
        check!(native_resolver.calls() == 1);
    }
}
