//! Metadata submission: pre-validation and KIP-631 encoding of a caller's
//! records against a scratch image, the append that assigns their offsets, and
//! the parked commit waiters that the high watermark later resolves or fails.

use krabka_ids::Offset;
use krabka_metadata::{
    BreakGlassProposalRecord, DelegationToken, DelegationTokenRecord, DeleteDelegationTokenRecord,
    MetadataImage, MetadataRecord, PartitionRecord, TopicFreezeRecord, from_kraft_value,
    to_kraft_values,
};
use tokio::sync::oneshot;

use super::{
    CommitWaiter, Engine, KraftController,
    offsets::{
        assigned_record_offset, hwm_reaches_waiter, leader_alone_is_majority,
        submit_waiter_need_offset, validate_append_result,
    },
    records::{decode_committed_value, metadata_record_batch, next_batch_offset},
};
use crate::{
    DelegationTokenMutation, OffsetReservation, SubmitChangeResult, error::RaftError,
    kraft::role::Role,
};

impl Engine {
    fn token_generation_matches(
        expected: &DelegationTokenRecord,
        replacement: &DelegationTokenRecord,
    ) -> bool {
        expected.token_id == replacement.token_id
            && expected.owner == replacement.owner
            && expected.requester == replacement.requester
            && expected.issue_timestamp_ms == replacement.issue_timestamp_ms
            && expected.max_timestamp_ms == replacement.max_timestamp_ms
            && expected.renewers == replacement.renewers
    }

    fn token_mutation_decision(
        image: &MetadataImage,
        mutation: &DelegationTokenMutation,
        now_ms: i64,
        uncommitted_tail: bool,
    ) -> krabka_verified::TokenMutationDecision {
        let (kind, expected, replacement) = match mutation {
            DelegationTokenMutation::Renew {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Renew,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Expire {
                expected,
                replacement,
            } => (
                krabka_verified::TokenMutationKind::Expire,
                expected,
                Some(replacement),
            ),
            DelegationTokenMutation::Delete { expected } => {
                (krabka_verified::TokenMutationKind::Delete, expected, None)
            }
        };
        let stored = image
            .delegation_token_by_id(&expected.token_id)
            .map(DelegationToken::to_record);
        let generation_matches = replacement
            .is_none_or(|replacement| Self::token_generation_matches(expected, replacement));
        let state = match (&stored, replacement, generation_matches) {
            (None, _, true) => krabka_verified::TokenMutationState::Missing,
            (Some(stored), Some(replacement), true) if stored == replacement => {
                krabka_verified::TokenMutationState::Applied
            }
            (Some(stored), _, true) if stored == expected => {
                krabka_verified::TokenMutationState::Expected
            }
            (_, _, false) | (Some(_), _, true) => krabka_verified::TokenMutationState::Stale,
        };
        krabka_verified::token_mutation_decision(krabka_verified::TokenMutationFacts {
            kind,
            state,
            now_ms,
            expected_expiry_ms: expected.expiry_timestamp_ms,
            incoming_expiry_ms: replacement.map_or(expected.expiry_timestamp_ms, |record| {
                record.expiry_timestamp_ms
            }),
            max_timestamp_ms: expected.max_timestamp_ms,
            uncommitted_tail,
        })
    }

    fn token_mutation_record(mutation: &DelegationTokenMutation) -> MetadataRecord {
        match mutation {
            DelegationTokenMutation::Renew { replacement, .. }
            | DelegationTokenMutation::Expire { replacement, .. } => {
                MetadataRecord::V1DelegationToken(replacement.clone())
            }
            DelegationTokenMutation::Delete { expected } => {
                MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
                    token_id: expected.token_id.clone(),
                })
            }
        }
    }

    fn token_mutation_id(mutation: &DelegationTokenMutation) -> &str {
        match mutation {
            DelegationTokenMutation::Renew { expected, .. }
            | DelegationTokenMutation::Expire { expected, .. }
            | DelegationTokenMutation::Delete { expected } => &expected.token_id,
        }
    }

    pub fn on_submit_delegation_token_mutations(
        &mut self,
        mutations: &[DelegationTokenMutation],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }

        let uncommitted_tail = self.log.hwm() < self.log.log_end_offset();
        let now_ms = KraftController::wall_clock_ms();
        let mut scratch = self.image.clone();
        let mut records = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let decision =
                Self::token_mutation_decision(&scratch, mutation, now_ms, uncommitted_tail);
            match decision {
                krabka_verified::TokenMutationDecision::Append => {
                    let record = Self::token_mutation_record(mutation);
                    scratch.apply(&record);
                    records.push(record);
                }
                krabka_verified::TokenMutationDecision::Retry => {}
                krabka_verified::TokenMutationDecision::Reject => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                        "delegation-token mutation {} rejected",
                        Self::token_mutation_id(mutation)
                    ))));
                    return;
                }
            }
        }
        if records.is_empty() {
            let _ = reply.send(Ok(SubmitChangeResult::default()));
            return;
        }
        self.on_submit_change_guarded(&records, reply, true);
    }

    fn consumption_matches_stored(
        stored: &BreakGlassProposalRecord,
        consumed: &BreakGlassProposalRecord,
    ) -> bool {
        let mut expected = stored.clone();
        expected.consumed_at_ms = consumed.consumed_at_ms;
        expected == *consumed
    }

    fn break_glass_consumption_decision(
        &self,
        consumed: &BreakGlassProposalRecord,
    ) -> krabka_verified::BreakGlassConsumptionDecision {
        let stored = self.image.break_glass_proposal(consumed.proposal_id);
        let proposal = match stored {
            None => krabka_verified::BreakGlassProposalState::Missing,
            Some(stored)
                if Self::consumption_matches_stored(stored, consumed)
                    && stored.consumed_at_ms == 0
                    && !stored.withdrawn =>
            {
                krabka_verified::BreakGlassProposalState::ExactPending
            }
            Some(_) => krabka_verified::BreakGlassProposalState::Stale,
        };
        krabka_verified::break_glass_consumption_decision(
            krabka_verified::BreakGlassConsumptionFacts {
                proposal,
                consumed_at_ms: consumed.consumed_at_ms,
                // A consume is a security-sensitive compare-and-set. Require
                // the committed image to cover the whole log prefix before
                // appending it. This conservative fence survives leadership
                // loss, where a waiter can disappear while its uncommitted
                // log entry remains and may later commit.
                uncommitted_tail: self.log.hwm() < self.log.log_end_offset(),
            },
        )
    }

    fn freeze_replacement_decision(
        &self,
        incoming: &TopicFreezeRecord,
        another_freeze_in_batch: bool,
    ) -> krabka_verified::FreezeReplacementDecision {
        let stored = self
            .image
            .topic_freezes()
            .find(|stored| {
                stored.pattern_type == incoming.pattern_type && stored.scope == incoming.scope
            })
            .map_or(krabka_verified::FreezeStoredState::Missing, |stored| {
                krabka_verified::FreezeStoredState::Present {
                    set_at_ms: stored.set_at_ms,
                }
            });
        krabka_verified::freeze_replacement_decision(krabka_verified::FreezeReplacementFacts {
            stored,
            incoming_frozen: incoming.frozen,
            incoming_set_at_ms: incoming.set_at_ms,
            // Like proposal consumption, replacement is a compare-and-set
            // against the committed image. A retained tail or a second
            // freeze in this batch must commit or fail before retry.
            uncommitted_tail: another_freeze_in_batch || self.log.hwm() < self.log.log_end_offset(),
        })
    }

    /// Refuse a batch whose compare-and-set records the committed image does
    /// not admit.
    ///
    /// A break-glass consume, a topic-freeze replacement, and an unguarded
    /// delegation-token write are checked against the committed image before
    /// append.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::UncommittedTail`] when only the uncommitted log
    /// tail stops a consume or a freeze replacement, and
    /// [`RaftError::ChangeRejected`] for every other refusal.
    fn check_compare_and_set_records(
        &self,
        records: &[MetadataRecord],
        delegation_token_guarded: bool,
    ) -> Result<(), RaftError> {
        let mut freeze_in_batch = false;
        let mut token_create_in_batch = std::collections::HashSet::new();
        for record in records {
            match record {
                MetadataRecord::V1BreakGlassProposal(consumed) if consumed.consumed_at_ms != 0 => {
                    match self.break_glass_consumption_decision(consumed) {
                        krabka_verified::BreakGlassConsumptionDecision::Append => {}
                        // Only an uncommitted tail gives `InFlight`. It clears
                        // when the tail commits, so the caller can retry.
                        krabka_verified::BreakGlassConsumptionDecision::InFlight => {
                            return Err(RaftError::UncommittedTail);
                        }
                        decision => {
                            return Err(RaftError::ChangeRejected(format!(
                                "break-glass consume {} rejected: {decision:?}",
                                consumed.proposal_id
                            )));
                        }
                    }
                }
                MetadataRecord::V1TopicFreeze(freeze) => {
                    match self.freeze_replacement_decision(freeze, freeze_in_batch) {
                        krabka_verified::FreezeReplacementDecision::Append => {}
                        // A second freeze in the same batch also gives
                        // `InFlight`. That batch fails again on every retry,
                        // so only the uncommitted tail is a transient refusal.
                        krabka_verified::FreezeReplacementDecision::InFlight
                            if !freeze_in_batch =>
                        {
                            return Err(RaftError::UncommittedTail);
                        }
                        decision => {
                            return Err(RaftError::ChangeRejected(format!(
                                "topic-freeze mutation {:?}:{} rejected: {decision:?}",
                                freeze.pattern_type, freeze.scope
                            )));
                        }
                    }
                    freeze_in_batch = true;
                }
                MetadataRecord::V1DelegationToken(token) if !delegation_token_guarded => {
                    let create_is_unique =
                        self.image.delegation_token_by_id(&token.token_id).is_none()
                            && token_create_in_batch.insert(token.token_id.clone());
                    if !create_is_unique || self.log.hwm() < self.log.log_end_offset() {
                        return Err(RaftError::ChangeRejected(format!(
                            "delegation-token create {} rejected: replacement requires a guarded mutation",
                            token.token_id
                        )));
                    }
                }
                MetadataRecord::V1BrokerRegistration(amend)
                    if amend.broker_epoch >= 0
                        && self.registration_change_must_wait(amend.node_id) =>
                {
                    return Err(RaftError::UncommittedTail);
                }
                MetadataRecord::V1BrokerRegistrationChange(change)
                    if self.registration_change_must_wait(change.node_id) =>
                {
                    return Err(RaftError::UncommittedTail);
                }
                MetadataRecord::V1DeleteDelegationToken(token) if !delegation_token_guarded => {
                    return Err(RaftError::ChangeRejected(format!(
                        "delegation-token delete {} rejected: mutation is not generation-bound",
                        token.token_id
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Replay the uncommitted records of earlier leader epochs into a copy of
    /// the committed image, and give each record to `visit` before it
    /// applies.
    ///
    /// The records between the high watermark and this leader's epoch start
    /// offset were appended by an earlier leader, possibly this node, and are
    /// not committed. They commit when this epoch's first record does. Each
    /// value is decoded against the image that the values before it produce,
    /// as a replica replays it. A value that names state the image does not
    /// hold, or that does not validate, is skipped, as replay skips it (see
    /// [`decode_committed_value`]). A node that does not lead has no such
    /// records, and gets the committed image.
    ///
    /// # Errors
    ///
    /// Returns the error of a log read that fails, and
    /// [`RaftError::MetadataReplay`] for a value that does not decode. `visit`
    /// has then seen the records before it. The value commits with this
    /// epoch's first record, and its apply then stops the controller.
    pub(super) fn replay_earlier_epoch_tail(
        &self,
        mut visit: impl FnMut(&MetadataImage, &MetadataRecord),
    ) -> Result<MetadataImage, RaftError> {
        let mut image = self.image.clone();
        let Role::Leader {
            epoch_start_offset, ..
        } = self.core.role()
        else {
            return Ok(image);
        };
        let end = Offset(*epoch_start_offset);
        let mut cursor = self.log.hwm();
        while cursor < end {
            let batches = self
                .log
                .read_decoded(cursor, self.metadata_raft_fetch_max.size())?;
            let Some(next) = next_batch_offset(&batches).filter(|next| *next > cursor) else {
                break;
            };
            for batch in &batches {
                if batch.base_offset >= end.0 || batch.attributes.is_control_batch() {
                    continue;
                }
                for (offset, value) in batch.records.iter().filter_map(|record| {
                    let offset = batch
                        .base_offset
                        .saturating_add(i64::from(record.offset_delta));
                    record.value.as_ref().map(|value| (offset, value))
                }) {
                    let Some(record) = decode_committed_value(value, &image, offset)? else {
                        continue;
                    };
                    if image.validate(&record).is_err() {
                        continue;
                    }
                    visit(&image, &record);
                    image.apply(&record);
                }
            }
            cursor = next;
        }
        Ok(image)
    }

    /// The names of the topics that uncommitted records from earlier leader
    /// epochs create, as [`Self::replay_earlier_epoch_tail`] replays them.
    fn earlier_epoch_topic_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        // A tail that does not read adds no more names, the same as a tail
        // that ends there.
        let _ = self.replay_earlier_epoch_tail(|image, record| {
            if let MetadataRecord::V1Topic(topic) = record
                && image.topic(&topic.name).is_none()
            {
                names.push(topic.name.clone());
            }
        });
        names
    }

    /// Whether a change to the registration of `node_id` must wait for the
    /// log to commit before the leader can decide it.
    ///
    /// A change names the broker epoch of the registration it was built from,
    /// and applies only while the broker is still registered at that epoch,
    /// as Kafka's `ClusterControlManager.replayRegistrationChange` refuses a
    /// `BrokerRegistrationChangeRecord` for any other epoch. The leader
    /// decides that against its committed image. A registration of the same
    /// broker this leader appended but has not committed yet is not in that
    /// image, and neither is anything a previous leader left uncommitted until
    /// this leader's own epoch commits; either could replace the registration
    /// the change names. The refusal clears when the tail commits, and the
    /// caller builds the change again from the image that holds it.
    fn registration_change_must_wait(&self, node_id: krabka_metadata::NodeId) -> bool {
        let epoch_ready = match self.core.role() {
            Role::Leader {
                epoch_start_offset, ..
            } => {
                krabka_verified::wal_reservation_epoch_ready(self.log.hwm().0, *epoch_start_offset)
            }
            _ => false,
        };
        let leader_epoch = self.core.quorum_state().leader_epoch;
        !epoch_ready
            || self
                .registration_writes
                .get(&node_id)
                .is_some_and(|&(epoch, end)| {
                    epoch == leader_epoch && !hwm_reaches_waiter(self.log.hwm(), end)
                })
    }

    /// Handle a `submit_change`: leader appends + parks a waiter; non-leader
    /// rejects immediately with the leader hint.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            node = self.me.0,
            epoch = self.core.quorum_state().leader_epoch,
            is_leader = self.core.role().is_leader(),
            records = records.len()
        )
    )]
    pub fn on_submit_change(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
    ) {
        self.on_submit_change_guarded(records, reply, false);
    }

    fn on_submit_change_guarded(
        &mut self,
        records: &[krabka_metadata::MetadataRecord],
        reply: oneshot::Sender<Result<SubmitChangeResult, RaftError>>,
        delegation_token_guarded: bool,
    ) {
        if !self.core.role().is_leader() {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
            return;
        }
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let epoch_ready = match self.core.role() {
            Role::Leader {
                epoch_start_offset, ..
            } => {
                krabka_verified::wal_reservation_epoch_ready(self.log.hwm().0, *epoch_start_offset)
            }
            _ => false,
        };
        // A new leader reserves no offsets until its own epoch commits. The
        // refusal is transient, as the refusal of a compare-and-set behind
        // an uncommitted tail is, so it is the same error. A broker then
        // tells the refusal apart from a failed reservation, and the forward
        // path keeps it.
        if records
            .iter()
            .any(|record| matches!(record, MetadataRecord::V1PartitionOffsetAdvance(_)))
            && !epoch_ready
        {
            let _ = reply.send(Err(RaftError::UncommittedTail));
            return;
        }

        if let Err(error) = self.check_compare_and_set_records(records, delegation_token_guarded) {
            let _ = reply.send(Err(error));
            return;
        }

        // Kafka's `QuorumController` replays each record into its in-memory
        // state before the record commits. Thus `ReplicationControlManager`
        // `createTopics` sees a pending topic name as an existing topic and
        // answers `TOPIC_ALREADY_EXISTS`. This leader validates against the
        // applied image only, so it checks the names that parked waiters
        // create.
        //
        // A leadership loss drops the waiters, but their records can stay in
        // the log. This leader never truncates its own log, so a record from
        // an earlier epoch commits with this epoch's first record. Its topic
        // names count as pending too.
        let creates = created_topic_names(&self.image, records);
        let earlier_epoch_creates = if creates.is_empty() {
            Vec::new()
        } else {
            self.earlier_epoch_topic_names()
        };
        if let Some(name) = creates.iter().find(|name| {
            earlier_epoch_creates.contains(name)
                || self
                    .commit_waiters
                    .iter()
                    .any(|waiter| waiter.creates.contains(name))
        }) {
            let _ = reply.send(Err(RaftError::Metadata(
                krabka_metadata::MetadataError::TopicExists(name.clone()),
            )));
            return;
        }

        // Pre-validate and translate to KIP-631 value blobs in ONE pass against
        // an evolving scratch image, so config-diff / ACL-resolution in
        // `to_kraft_values` see in-batch prior records (a batch mixing
        // topic+partition is validated and encoded as a sequence).

        // KIP-903: broker epoch = the offset this batch commits at. The i-th
        // value blob lands at `assign_base + i`; a V1BrokerRegistration fans
        // out to exactly one blob, so its offset delta equals the number of
        // blobs already allocated. Single-writer leader: the current log end
        // offset is the base `append` will return.
        let assign_base = self.log.log_end_offset();

        let mut scratch = self.image.clone();
        // What a replica sees when it replays this batch: each value blob
        // decoded against the image the blobs before it produced. A blob that
        // does not decode here would be dropped by every replica, and a Kafka
        // replica would fail on it, so the batch is refused before it reaches
        // the log.
        let mut replica_view = self.image.clone();
        let mut result = SubmitChangeResult::default();
        let mut value_blobs: Vec<bytes::Bytes> = Vec::new();
        // The brokers whose registration this batch writes.
        let mut registration_nodes = Vec::new();
        for r in records {
            let rebased = rebase_partition_directories(&scratch, r);
            let r = rebased.as_ref().unwrap_or(r);
            // A new registration carries no epoch (-1) and is stamped with
            // its committed offset. An amend of the registration (a
            // `RegisterBrokerRecord` at the epoch it already holds) and a
            // `BrokerRegistrationChangeRecord` apply only while the broker is
            // registered at the epoch they name, as Kafka's
            // `ClusterControlManager.replayRegistrationChange` requires. One
            // built from a registration that has since been replaced is
            // dropped rather than failing its batch: it must neither
            // overwrite the new registration nor register again, and the
            // partition changes it travels with still hold.
            let stamped;
            let r: &MetadataRecord = match r {
                MetadataRecord::V1BrokerRegistration(b) if b.broker_epoch < 0 => {
                    registration_nodes.push(b.node_id);
                    let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
                    let mut b = b.clone();
                    b.broker_epoch = assigned_record_offset(assign_base, delta);
                    stamped = MetadataRecord::V1BrokerRegistration(b);
                    &stamped
                }
                MetadataRecord::V1BrokerRegistration(b) => {
                    if !registered_at(&scratch, b.node_id, b.broker_epoch, Some(b.incarnation_id)) {
                        continue;
                    }
                    registration_nodes.push(b.node_id);
                    r
                }
                MetadataRecord::V1UnregisterBroker(unregister) => {
                    registration_nodes.push(unregister.node_id);
                    r
                }
                MetadataRecord::V1BrokerRegistrationChange(change) => {
                    if !registered_at(&scratch, change.node_id, change.broker_epoch, None) {
                        continue;
                    }
                    registration_nodes.push(change.node_id);
                    r
                }
                other => other,
            };
            if let Err(e) = scratch.validate(r) {
                let _ = reply.send(Err(RaftError::Metadata(e)));
                return;
            }
            if let MetadataRecord::V1PartitionOffsetAdvance(r) = r {
                let mut next_offset = scratch
                    .partition_next_offset(&r.topic, r.partition)
                    .unwrap_or(0);
                // A multi-voter leader may have earlier reservations appended
                // but not committed into `scratch` yet. Fold their exact
                // contiguous ends so concurrent submissions cannot reuse the
                // same committed base.
                for pending in self.commit_waiters.iter().flat_map(|waiter| {
                    waiter.result.offset_reservations.iter().filter(|pending| {
                        pending.topic == r.topic && pending.partition == r.partition
                    })
                }) {
                    let Some(frontier) = krabka_verified::wal_reservation_frontier(
                        next_offset,
                        pending.base_offset,
                        pending.count,
                    ) else {
                        let _ = reply.send(Err(RaftError::ChangeRejected(
                            "pending offset reservation chain is invalid".to_string(),
                        )));
                        return;
                    };
                    next_offset = frontier;
                }
                // The proved reservation admits exactly a nonnegative frontier,
                // a positive count, and an end that fits i64; untrusted
                // metadata outside that is rejected.
                let Some((base_offset, _next_offset)) =
                    krabka_verified::reserve_offsets(next_offset, r.count)
                else {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!(
                        "partition offset advance count {} is out of range at next offset {next_offset}",
                        r.count
                    ))));
                    return;
                };
                result.offset_reservations.push(OffsetReservation {
                    topic: r.topic.clone(),
                    partition: r.partition,
                    base_offset,
                    count: r.count,
                    leader_epoch: u64::from(leader_epoch),
                });
            }
            match to_kraft_values(r, &scratch) {
                Ok(mut blobs) => {
                    if let Err(e) = replay_value_blobs(&blobs, &mut replica_view) {
                        let _ = reply.send(Err(e));
                        return;
                    }
                    value_blobs.append(&mut blobs);
                }
                Err(e) => {
                    let _ = reply.send(Err(RaftError::ChangeRejected(format!("encode: {e}"))));
                    return;
                }
            }
            scratch.apply(r);
        }

        // Every record fanned out to nothing (e.g. an empty config clear): the
        // submit is a committed no-op. Reply success without appending a batch.
        if value_blobs.is_empty() {
            let _ = reply.send(Ok(result));
            return;
        }

        let mut batch = match metadata_record_batch(leader_epoch, &value_blobs) {
            Ok(batch) => batch,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        let base = match self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())
        {
            Ok(off) => off,
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };
        if let Err(e) = validate_append_result(
            "submit-change",
            assign_base,
            base,
            self.log.log_end_offset(),
        ) {
            let _ = reply.send(Err(e));
            return;
        }
        let need_offset = submit_waiter_need_offset(base, value_blobs.len());
        for node_id in registration_nodes {
            self.registration_writes
                .insert(node_id, (leader_epoch, need_offset));
        }
        // Park the waiter, then try to advance the HWM immediately: a single
        // voter commits its own append with no peer fetch.
        self.commit_waiters.push(CommitWaiter {
            base_offset: base,
            need_offset,
            rejection: None,
            creates,
            result,
            reply,
        });
        // Drive a self-fetch so the core recomputes the HWM (single voter
        // commits immediately; multi-voter commits when followers fetch).
        if leader_alone_is_majority(self.core.quorum_state().majority(), self.core.is_voter()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
        self.try_resolve_waiters();
    }

    /// Test-only: append a metadata batch and commit it through the real apply
    /// pipeline. Returns the appended base offset (or -1 on failure).
    #[cfg(test)]
    pub fn test_append_and_commit(&mut self, records: &[krabka_metadata::MetadataRecord]) -> i64 {
        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut scratch = self.image.clone();
        let mut blobs: Vec<bytes::Bytes> = Vec::new();
        for r in records {
            if let Ok(mut bs) = to_kraft_values(r, &scratch) {
                blobs.append(&mut bs);
            }
            scratch.apply(r);
        }
        let mut batch = match metadata_record_batch(leader_epoch, &blobs) {
            Ok(batch) => batch,
            Err(e) => {
                tracing::error!(?e, "kraft: test batch construction failed");
                return -1;
            }
        };
        let expected_base = self.log.log_end_offset();
        let base = match self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())
        {
            Ok(off) => off,
            Err(e) => {
                tracing::error!(?e, "kraft: test append failed");
                return -1;
            }
        };
        if let Err(e) = validate_append_result(
            "test append",
            expected_base,
            base,
            self.log.log_end_offset(),
        ) {
            tracing::error!(?e, "kraft: test append invariant failed");
            return -1;
        }
        self.advance_and_apply(self.log.log_end_offset());
        // Test helper returns the raw base offset (compared against `-1` sentinel).
        base.0
    }

    /// Attach a rejection to the waiter whose appended range
    /// `[base_offset, need_offset)` actually contains `record_offset`. Gating on
    /// both bounds (not just `need_offset > record_offset`) prevents a failing
    /// record from bleeding its rejection onto later, unrelated waiters whose
    /// own records committed fine (FIX 2).
    pub fn note_rejection(&mut self, record_offset: Offset, err: &krabka_metadata::MetadataError) {
        for w in &mut self.commit_waiters {
            if w.base_offset <= record_offset
                && record_offset < w.need_offset
                && w.rejection.is_none()
            {
                w.rejection = Some(RaftError::Metadata(err.clone()));
            }
        }
    }

    /// Resolve every waiter whose target offset is now committed.
    pub fn try_resolve_waiters(&mut self) {
        let hwm = self.log.hwm();
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let result = w.rejection.map_or(Ok(w.result), Err);
                let _ = w.reply.send(result);
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }

    pub fn fail_waiters_reached_by(&mut self, hwm: Offset, reason: &str) {
        let mut still = Vec::new();
        for w in self.commit_waiters.drain(..) {
            if hwm_reaches_waiter(hwm, w.need_offset) {
                let _ = w
                    .reply
                    .send(Err(RaftError::ChangeRejected(reason.to_string())));
            } else {
                still.push(w);
            }
        }
        self.commit_waiters = still;
    }
}

/// KIP-858: while a partition keeps its replica list, only
/// `AssignReplicasToDirs` moves its `directories`.
///
/// A caller that writes a whole partition record (an election, an ISR change)
/// copies `directories` from the image it read. That image can predate a
/// directory assignment that has since committed. Written as it is, the record
/// would revert the assignment, and a stale empty list would encode a
/// `PartitionChangeRecord` whose directory count does not match its replicas,
/// which no replica can apply. So a record that keeps the replica list takes
/// the directories of the image it is encoded against. Kafka's controller
/// behaves the same way. `PartitionChangeBuilder` never takes directories from
/// the caller of an election or an ISR change: it derives them from the
/// partition's current directories, keyed by replica, and only the
/// `AssignReplicasToDirs` handler calls `setDirectory`.
///
/// Returns `None` when the record needs no change.
fn rebase_partition_directories(
    image: &MetadataImage,
    record: &MetadataRecord,
) -> Option<MetadataRecord> {
    let rebase = |partition: &PartitionRecord| -> Option<PartitionRecord> {
        let current = image.partition(&partition.topic, partition.partition)?;
        (current.replicas == partition.replicas && current.directories != partition.directories)
            .then(|| PartitionRecord {
                directories: current.directories.clone(),
                ..partition.clone()
            })
    };
    match record {
        MetadataRecord::V1Partition(partition) => {
            rebase(partition).map(MetadataRecord::V1Partition)
        }
        MetadataRecord::V1PartitionUpdate(update) => rebase(&update.partition).map(|partition| {
            MetadataRecord::V1PartitionUpdate(krabka_metadata::PartitionUpdateRecord {
                partition,
                ..update.clone()
            })
        }),
        _ => None,
    }
}

/// The names of the topics that `records` create: each `V1Topic` name that
/// `image` does not have. A `V1Topic` for a topic that `image` has is a
/// partition growth or a rejected re-create, so its name is not included.
/// Each name occurs one time only.
fn created_topic_names(image: &MetadataImage, records: &[MetadataRecord]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for record in records {
        if let MetadataRecord::V1Topic(topic) = record
            && image.topic(&topic.name).is_none()
            && !names.contains(&topic.name)
        {
            names.push(topic.name.clone());
        }
    }
    names
}

/// Decode `blobs` the way a replica replays them, each against the image the
/// blobs before it produced, and apply each to `image`.
///
/// # Errors
///
/// Returns [`RaftError::ChangeRejected`] for the first blob that does not
/// decode.
/// Whether `node_id` is registered in `image` at `broker_epoch`, and, when
/// `incarnation_id` is given, as that incarnation: the condition under which
/// an amend or a `BrokerRegistrationChangeRecord` still applies. A change that
/// fails it is logged, and the caller drops it.
fn registered_at(
    image: &MetadataImage,
    node_id: krabka_metadata::NodeId,
    broker_epoch: i64,
    incarnation_id: Option<uuid::Uuid>,
) -> bool {
    let registered = image.broker(node_id).is_some_and(|existing| {
        existing.broker_epoch == broker_epoch
            && incarnation_id.is_none_or(|incarnation| existing.incarnation_id == incarnation)
    });
    if !registered {
        tracing::warn!(
            broker = node_id.0,
            broker_epoch,
            registered_epoch = ?image.broker_epoch(node_id),
            "dropping a broker registration change for an epoch the broker is no longer \
             registered at"
        );
    }
    registered
}

fn replay_value_blobs(blobs: &[bytes::Bytes], image: &mut MetadataImage) -> Result<(), RaftError> {
    for blob in blobs {
        let decoded = from_kraft_value(blob, image).map_err(|e| {
            RaftError::ChangeRejected(format!("encoded record would not replay: {e}"))
        })?;
        image.apply(&decoded);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use krabka_metadata::{DelegationTokenRecord, MetadataRecord, PartitionRecord};
    use krabka_security::KafkaPrincipal;
    use uuid::Uuid;

    use super::*;
    use crate::types::NodeId;

    fn principal(name: &str) -> KafkaPrincipal {
        KafkaPrincipal {
            principal_type: "User".to_string(),
            name: name.to_string(),
        }
    }

    #[test]
    fn wall_clock_ms_is_real_timestamp() {
        assert2::check!(KraftController::wall_clock_ms() > 1_700_000_000_000);
    }

    #[test]
    fn token_generation_matches_checks_all_fields() {
        let t1 = DelegationTokenRecord {
            token_id: "tok1".into(),
            owner: principal("alice"),
            requester: principal("alice"),
            issue_timestamp_ms: 100,
            max_timestamp_ms: 200,
            expiry_timestamp_ms: 150,
            renewers: vec![principal("bob")],
        };
        let mut t2 = t1.clone();
        assert2::check!(Engine::token_generation_matches(&t1, &t2));

        t2.token_id = "tok2".into();
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.owner = principal("charlie");
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.requester = principal("admin");
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.issue_timestamp_ms = 101;
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.max_timestamp_ms = 201;
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
        t2 = t1.clone();

        t2.renewers = vec![principal("dan")];
        assert2::check!(!Engine::token_generation_matches(&t1, &t2));
    }

    #[test]
    fn token_mutation_id_extracts_token_id() {
        let rec = DelegationTokenRecord {
            token_id: "my-token".into(),
            owner: principal("alice"),
            requester: principal("alice"),
            issue_timestamp_ms: 0,
            max_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            renewers: vec![],
        };
        let m1 = DelegationTokenMutation::Renew {
            expected: rec.clone(),
            replacement: rec.clone(),
        };
        assert2::check!(Engine::token_mutation_id(&m1) == "my-token");

        let m2 = DelegationTokenMutation::Expire {
            expected: rec.clone(),
            replacement: rec.clone(),
        };
        assert2::check!(Engine::token_mutation_id(&m2) == "my-token");

        let m3 = DelegationTokenMutation::Delete { expected: rec };
        assert2::check!(Engine::token_mutation_id(&m3) == "my-token");
    }

    #[test]
    fn rebase_partition_directories_copies_image_directories() {
        let mut image = MetadataImage::default();
        let topic_id = Uuid::from_u128(42);
        image.apply(&MetadataRecord::V1Topic(
            crate::test_support::single_partition_topic("test-topic", topic_id),
        ));
        let dir_id = Uuid::from_u128(99);
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            directories: vec![dir_id],
            ..crate::test_support::single_replica_partition(
                "test-topic",
                krabka_ids::PartitionIndex(0),
                NodeId(1),
            )
        }));

        let new_part = PartitionRecord {
            leader_epoch: krabka_metadata::LeaderEpoch(1),
            partition_epoch: 1,
            ..crate::test_support::single_replica_partition(
                "test-topic",
                krabka_ids::PartitionIndex(0),
                NodeId(1),
            )
        };

        let rebased = rebase_partition_directories(&image, &MetadataRecord::V1Partition(new_part));
        if let Some(MetadataRecord::V1Partition(p)) = rebased {
            assert2::check!(p.directories == vec![dir_id]);
        } else {
            panic!("expected V1Partition");
        }
    }

    #[test]
    fn created_topic_names_lists_only_names_absent_from_the_image() {
        let topic = |name: &str, id: u128| {
            MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
                name,
                Uuid::from_u128(id),
            ))
        };
        let mut image = MetadataImage::default();
        image.apply(&topic("existing", 1));

        let cases: [(&str, Vec<MetadataRecord>, Vec<&str>); 4] = [
            ("new topic", vec![topic("new", 2)], vec!["new"]),
            ("existing topic", vec![topic("existing", 1)], vec![]),
            (
                "name repeated in one batch",
                vec![topic("new", 2), topic("new", 3), topic("other", 4)],
                vec!["new", "other"],
            ),
            (
                "no topic records",
                vec![MetadataRecord::V1DeleteTopic(
                    krabka_metadata::DeleteTopicRecord {
                        name: "existing".into(),
                    },
                )],
                vec![],
            ),
        ];
        for (case, records, expected) in cases {
            assert2::check!(
                created_topic_names(&image, &records) == expected,
                "case: {case}"
            );
        }
    }

    #[test]
    fn unguarded_delegation_token_records_validation() {
        use krabka_metadata::DeleteDelegationTokenRecord;

        use crate::kraft::controller::test_support::{build_engine_only, one_offset_batch};

        let (mut engine, _dir) =
            build_engine_only(crate::kraft::controller::test_support::EngineSetup {
                ids: &[NodeId(1)],
                ..Default::default()
            });
        let t1 = DelegationTokenRecord {
            token_id: "tok1".into(),
            owner: principal("alice"),
            requester: principal("alice"),
            issue_timestamp_ms: 100,
            max_timestamp_ms: 200,
            expiry_timestamp_ms: 150,
            renewers: vec![],
        };
        let tok = MetadataRecord::V1DelegationToken(t1);

        // Unguarded delete is always rejected
        let del = MetadataRecord::V1DeleteDelegationToken(DeleteDelegationTokenRecord {
            token_id: "tok1".into(),
        });
        let res = engine.check_compare_and_set_records(&[del], false);
        assert2::assert!(matches!(res, Err(RaftError::ChangeRejected(_))));

        // Duplicate token create in the same batch is rejected
        let res_dup = engine.check_compare_and_set_records(&[tok.clone(), tok.clone()], false);
        assert2::assert!(matches!(res_dup, Err(RaftError::ChangeRejected(_))));

        // Token that already exists in image is rejected
        engine.image.apply(&tok);
        let res_exists = engine.check_compare_and_set_records(std::slice::from_ref(&tok), false);
        assert2::assert!(matches!(res_exists, Err(RaftError::ChangeRejected(_))));

        // Token create when hwm < log_end_offset (uncommitted tail) is rejected
        let (mut engine2, _dir2) =
            build_engine_only(crate::kraft::controller::test_support::EngineSetup {
                ids: &[NodeId(1)],
                ..Default::default()
            });
        let t2 = DelegationTokenRecord {
            token_id: "tok2".into(),
            owner: principal("bob"),
            requester: principal("bob"),
            issue_timestamp_ms: 0,
            max_timestamp_ms: 0,
            expiry_timestamp_ms: 0,
            renewers: vec![],
        };
        let tok2 = MetadataRecord::V1DelegationToken(t2);
        let mut batch = one_offset_batch(0, 0, b"uncommitted");
        engine2.log.append(&mut batch, 0).unwrap();
        assert2::assert!(engine2.log.hwm() < engine2.log.log_end_offset());
        let res_tail = engine2.check_compare_and_set_records(std::slice::from_ref(&tok2), false);
        assert2::assert!(matches!(res_tail, Err(RaftError::ChangeRejected(_))));

        // When delegation_token_guarded is true, create is admitted
        let res_guarded = engine2.check_compare_and_set_records(&[tok2], true);
        assert2::assert!(res_guarded.is_ok());
    }
}
