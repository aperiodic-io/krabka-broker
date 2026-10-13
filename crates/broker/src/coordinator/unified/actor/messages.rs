//! The actor mailbox vocabulary: the [`GroupActorMessage`] command enum and
//! the structured results the parking classic RPCs reply with, kept together
//! because every handler module speaks this one protocol.

use {
    super::{ClassicView, DescribeView, ErrorCode, ReapOutcome},
    crate::{
        codes,
        coordinator::{
            DeleteGroupError, GroupSnapshot,
            topic_deletion::DeletedTopic,
            unified::{
                GroupSeed,
                classic_state::OffsetEntry,
                group::{CoordinatorGroup, GroupOffsets, OffsetFetchMember},
                regex_resolver::TopicRegexResolver,
            },
        },
    },
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
            heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequest,
            leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
            sync_group_request::SyncGroupRequest,
        },
        records::RecordBatch,
    },
    tokio::sync::oneshot,
};

#[derive(Debug)]
pub enum GroupActorMessage {
    // ── next-gen consumer protocol (non-parking) ──
    Heartbeat {
        request: ConsumerGroupHeartbeatRequest,
        client_id: String,
        client_host: String,
        /// Resolves the group's regular expressions against the metadata image
        /// the handler read and with this principal, as Kafka's
        /// `TopicRegexResolver` does with the request context of the
        /// heartbeat. The actor calls it only when the group's resolutions
        /// need to be made or refreshed.
        regex_resolver: std::sync::Arc<dyn TopicRegexResolver>,
        reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
    },
    /// Validate an `OffsetCommit` or `TxnOffsetCommit` against the group's
    /// LIVE protocol. The actor dispatches on `group.kind`. Next-gen checks
    /// the member epoch and, for an older epoch, each partition's assignment
    /// epoch. Classic checks member, instance, and generation. `Ok(())`
    /// allows the commit and `Err(code)` rejects it.
    ValidateCommit {
        commit: super::commit_validation::CommitRequest,
        reply: oneshot::Sender<Result<(), ErrorCode>>,
    },
    Describe {
        reply: oneshot::Sender<DescribeView>,
    },

    // ── classic protocol (parking) ──
    ClassicJoin {
        req: JoinGroupRequest,
        version: i16,
        client_id: String,
        client_host: String,
        /// Resolves a consumer group's regular expressions with this
        /// principal, as Kafka's `maybeUpdateRegularExpressions` does with the
        /// request context of a classic join to a consumer group.
        regex_resolver: std::sync::Arc<dyn TopicRegexResolver>,
        reply: oneshot::Sender<JoinResult>,
    },
    ClassicSync {
        req: SyncGroupRequest,
        reply: oneshot::Sender<SyncResult>,
    },
    ClassicHeartbeat {
        req: HeartbeatRequest,
        reply: oneshot::Sender<ErrorCode>,
    },
    ClassicLeave {
        req: LeaveGroupRequest,
        version: i16,
        reply: oneshot::Sender<LeaveResult>,
    },
    /// Atomically verify that a classic group is empty and append the
    /// tombstones of its offsets and its k2 record. A successful delete stops
    /// the actor.
    ClassicDelete {
        reply: oneshot::Sender<Result<(), DeleteGroupError>>,
    },
    /// Read-only classic snapshot for the admin handlers.
    ClassicInspect {
        reply: oneshot::Sender<ClassicView>,
    },
    /// `OffsetDelete`'s group check on the LIVE group, whatever its kind:
    /// Kafka's `validateOffsetDelete` error, or the topics the group
    /// subscribes to.
    OffsetDeleteGuard {
        reply: oneshot::Sender<Result<super::SubscribedTopics, ErrorCode>>,
    },
    /// Kind-agnostic admin snapshot for the classic `ListGroups` and
    /// `DescribeGroups` path. It projects the LIVE group into a
    /// `GroupSnapshot`, whether that group is classic or consumer, and
    /// including a hosted-classic group after migration. A migrated group
    /// therefore reports coherently whatever the handle's spawn-time `kind`
    /// hint holds.
    InspectAny {
        reply: oneshot::Sender<GroupSnapshot>,
    },

    // ── committed offsets (protocol-agnostic; on `Group.committed_offsets`) ──
    /// Append one `OffsetCommit`'s records to `__consumer_offsets` and apply
    /// them to the group, both inside the actor.
    ///
    /// The append cannot happen outside the mailbox. The KIP-211 sweep decides
    /// what to tombstone from the in-memory map and appends its tombstones in
    /// the same turn, so a commit that wrote its record first and queued the
    /// in-memory update afterwards could have the sweep read the stale map,
    /// tombstone the offset it had just acknowledged, and stop the actor
    /// before the update was ever applied. One message keeps the two appends
    /// in one order.
    CommitOffsets {
        batch: RecordBatch,
        entries: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<Result<(), ErrorCode>>,
    },
    UpdateCommitted {
        entries: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<()>,
    },
    /// Read the group's stable offsets and its unresolved transactional keys
    /// in one turn, for `OffsetFetch`.
    FetchOffsets {
        reply: oneshot::Sender<GroupOffsets>,
    },
    /// `FetchOffsets` for an `OffsetFetch` request that names a member (v9+).
    /// A consumer group validates the member first, as Kafka's
    /// `ConsumerGroup.validateOffsetFetch` does, and replies with the error
    /// code when it refuses the fetch.
    FetchOffsetsForMember {
        member: OffsetFetchMember,
        reply: oneshot::Sender<Result<GroupOffsets, ErrorCode>>,
    },
    RemoveCommitted {
        keys: Vec<(String, i32)>,
        reply: oneshot::Sender<()>,
    },
    /// KIP-211: tombstone every committed offset that has fallen out of
    /// retention, and the group with them when it keeps none. The sweep in
    /// `coordinator::retention` sends it to each group whose
    /// `__consumer_offsets` partition this broker leads.
    ReapExpiredOffsets {
        /// The sweep's clock reading, shared by every group in one pass.
        now_ms: i64,
        /// `offsets.retention.minutes` in milliseconds.
        retention_ms: i64,
        /// How long a group must have been memberless before the sweep may
        /// delete it for holding no offsets at all —
        /// `offsets.retention.check.interval.ms`, so a group an actor has just
        /// been spawned for gets its first message in before it is judged.
        empty_grace_ms: i64,
        reply: oneshot::Sender<ReapOutcome>,
    },

    /// Tombstone the committed offsets and the open transactional offsets of
    /// `topics`, which the metadata image no longer holds. The reply names
    /// the `(topic, partition)` keys that the actor tombstoned, sorted, and is
    /// empty when the append fails.
    DeleteTopicOffsets {
        /// The name and the topic id of each deleted topic.
        topics: Vec<DeletedTopic>,
        reply: oneshot::Sender<Vec<(String, i32)>>,
    },
    /// Kafka's `GroupMetadataManager.onMetadataUpdate` for this group: the
    /// metadata image created, changed or deleted `topics`. A consumer group
    /// that subscribes to one of them computes its metadata hash again at its
    /// next heartbeat. A classic group ignores the message, as Kafka's
    /// `ClassicGroup.requestMetadataRefresh` does nothing.
    MetadataUpdate {
        /// The names of the created, changed and deleted topics.
        topics: std::sync::Arc<[String]>,
    },

    // ── in-flight transactional offsets (KIP-447) ──
    /// Record that `producer_id`'s open transaction has durably written
    /// offset commits for `keys` at offsets-log position `written_at`. Until
    /// its marker arrives, an `OffsetFetch` with `require_stable = true`
    /// answers `UNSTABLE_OFFSET_COMMIT` for them.
    ///
    /// `written_at` orders the mark against the producer's markers, which the
    /// sender cannot do for itself: it marks after its append is durable, so a
    /// marker for the very transaction it is marking can be resolved here in
    /// between.
    AddPendingTxnOffsets {
        producer_id: i64,
        written_at: i64,
        keys: Vec<(String, i32)>,
        reply: oneshot::Sender<()>,
    },
    /// Reserve or release the keys a `TxnOffsetCommit` is about to append.
    TxnOffsetReservation(TxnOffsetReservation),
    /// Resolve `producer_id`'s transaction, whose marker is at offsets-log
    /// position `resolved_through`: publish `committed` and drop the
    /// producer's pending marks in the same turn. An abort marker sends an
    /// empty `committed`, which leaves the group's stable offsets as they
    /// were. Doing both in one message is what stops a `require_stable` fetch
    /// from seeing a partition that is neither pending nor yet updated.
    ResolveTxnOffsets {
        producer_id: i64,
        resolved_through: i64,
        committed: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<()>,
    },

    // ── bootstrap / lifecycle ──
    Seed(GroupSeed),
    /// Replace this (classic) actor's whole `Group` with replayed state.
    ClassicSeed(Box<CoordinatorGroup>),
    Shutdown(oneshot::Sender<()>),

    /// Test-only: flip the live `Group` to a fresh empty consumer group in
    /// place. This exercises the tick's dispatch on the live `group.kind`.
    #[cfg(test)]
    TestForceConsumerKind,

    /// Test-only: read back the moment the actor stamped this group
    /// memberless.
    ///
    /// The actor maintains `empty_since_ms` at the end of every turn, so a
    /// retention test that wants to sweep exactly one grace period after that
    /// stamp cannot read the wall clock for it — the stamp lands whenever the
    /// actor next runs, which on a loaded machine is after any instant the
    /// test read. Asking the actor orders the two.
    #[cfg(test)]
    TestEmptySinceMs {
        reply: oneshot::Sender<Option<i64>>,
    },
}

/// A `TxnOffsetCommit` reservation: the keys of `producer_id` that the handler
/// reserves before its transactional append, or releases after a failed one.
///
/// The reservation is what orders the append against `DeleteGroups` on the
/// actor: a delete that runs after the reservation tombstones the keys, and a
/// delete that runs before it stops the actor, so the reservation fails and
/// nothing is appended.
#[derive(Debug)]
pub struct TxnOffsetReservation {
    pub producer_id: i64,
    pub keys: Vec<(String, i32)>,
    /// `true` reserves the keys, `false` releases them.
    pub reserve: bool,
    pub reply: oneshot::Sender<()>,
}

impl TxnOffsetReservation {
    /// Applies the reservation and replies. Returns the actor's keep-running
    /// flag, which is always `true`.
    pub(super) fn apply(self, group: &mut CoordinatorGroup) -> bool {
        if self.reserve {
            group.reserve_txn_offsets(self.producer_id, self.keys);
        } else {
            group.release_txn_offsets(self.producer_id, &self.keys);
        }
        let _ = self.reply.send(());
        true
    }
}

/// Structured `JoinGroup` result for the handler, which encodes it for the
/// wire version. It mirrors the fields of `JoinGroupResponse`.
#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub struct JoinResult {
    pub error_code: ErrorCode,
    #[default(-1)]
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    /// KIP-814: the leader must not run the assignor. Kafka sets it only for
    /// a static leader that rejoins a `Stable` group at `JoinGroup` v9+.
    pub skip_assignment: bool,
    pub member_id: String,
    pub members: Vec<JoinResultMember>,
}

/// The defaults of Kafka's `JoinGroupResponseData`, generation `-1` included,
/// so an error reply carries the same fields Kafka's does.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinResultMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

/// Structured `SyncGroup` result for the handler.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncResult {
    pub error_code: ErrorCode,
    pub assignment: Bytes,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
}

/// Structured `LeaveGroup` result. Versions 0–2 use the top-level error;
/// versions 3+ use the per-member results.
#[derive(Debug, Default)]
pub struct LeaveResult {
    pub error_code: ErrorCode,
    pub members: Vec<MemberResponse>,
}

pub(super) fn classic_leave_result(
    version: i16,
    result: Result<Vec<MemberResponse>, ErrorCode>,
) -> LeaveResult {
    match result {
        Ok(members) => match version {
            ..=2 => LeaveResult {
                error_code: members
                    .first()
                    .map_or(codes::NONE, |member| member.error_code),
                members,
            },
            _ => LeaveResult {
                error_code: codes::NONE,
                members,
            },
        },
        Err(error_code) => LeaveResult {
            error_code,
            members: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn classic_leave_result_uses_the_wire_versions_error_shape() {
        let legacy = classic_leave_result(
            2,
            Ok(vec![MemberResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }]),
        );
        check!(legacy.error_code == codes::UNKNOWN_MEMBER_ID);
        check!(legacy.members[0].error_code == codes::UNKNOWN_MEMBER_ID);

        let batched = classic_leave_result(
            3,
            Ok(vec![MemberResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }]),
        );
        check!(batched.error_code == codes::NONE);
        check!(batched.members[0].error_code == codes::UNKNOWN_MEMBER_ID);

        let failed = classic_leave_result(3, Err(codes::COORDINATOR_LOAD_IN_PROGRESS));
        check!(failed.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        check!(failed.members.is_empty());
    }
}
