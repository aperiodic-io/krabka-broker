//! KIP-853 reconfiguration: validation and append of one voter or
//! `kraft.version` control operation, and the control-record bookkeeping that
//! tracks it from append through truncation to commitment.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_metadata::{MetadataRecord, VoterSet, VotersRecord};
use krabka_protocol::{
    owned::k_raft_version_record::KRaftVersionRecord as WireKRaftVersionRecord,
    records::{RecordBatch, metadata::control::ControlRecord},
};
use krabka_verified::reconfiguration::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationDecision, voter_reconfiguration_decision,
};
use tokio::sync::oneshot;

use super::{
    Engine, KraftController, PendingReconfig,
    control_state::{voter_set_to_wire, voter_supports_version},
    offsets::{hwm_reaches_waiter, leader_alone_is_majority, validate_append_result},
    records::{decode_control_record, start_of_epoch_batch, typed_control_batch},
};
use crate::{
    NodeId,
    error::RaftError,
    kraft::{
        role::Role,
        types::{Epoch, ReplicaKey},
    },
    reconfig::{ReconfigOutcome, VoterChange},
};

/// Classify the request's voter key against the committed voter set, with a
/// nil directory standing for Kafka's absent directory id on either side.
///
/// Kafka compares whole `ReplicaKey`s, so two absent directory ids are equal.
/// The RPC handlers refuse a request without a directory id before this runs;
/// only an in-process change for a statically configured voter reaches the
/// nil-to-nil match.
fn target_membership(current: &VoterSet, id: NodeId, directory_id: uuid::Uuid) -> TargetMembership {
    match current.get(id) {
        None => TargetMembership::Absent,
        Some(voter) if voter.directory_id == directory_id => TargetMembership::PresentSameDirectory,
        Some(voter) if voter.directory_id.is_nil() => TargetMembership::PresentUnknownDirectory,
        Some(_) => TargetMembership::PresentOtherDirectory,
    }
}

fn rejected_reconfiguration(
    decision: VoterReconfigurationDecision,
    leader: Option<NodeId>,
    target_id: Option<NodeId>,
    current_version: u16,
    requested_version: u16,
    lag: u64,
) -> Result<ReconfigOutcome, RaftError> {
    match decision {
        VoterReconfigurationDecision::NotLeader => Ok(ReconfigOutcome::NotLeader { leader }),
        VoterReconfigurationDecision::InProgress
        | VoterReconfigurationDecision::EpochUncommitted => Err(RaftError::ReconfigInProgress),
        VoterReconfigurationDecision::EmptyCurrentVoterSet => Err(RaftError::ReconfigRejected(
            "cannot reconfigure an empty voter set".into(),
        )),
        VoterReconfigurationDecision::UnsupportedKraftVersion => {
            Err(RaftError::UnsupportedKraftVersion(current_version))
        }
        VoterReconfigurationDecision::DuplicateVoter => Err(target_id.map_or_else(
            || RaftError::ReconfigRejected("duplicate voter without a voter id".into()),
            RaftError::DuplicateVoter,
        )),
        VoterReconfigurationDecision::IncompatibleVoter => {
            let message = target_id.map_or_else(
                || format!("not every voter supports kraft.version {requested_version}"),
                |id| format!("voter {id} does not support kraft.version {current_version}"),
            );
            Err(RaftError::InvalidVoterUpdate(message))
        }
        VoterReconfigurationDecision::VoterNotCaughtUp => target_id.map_or_else(
            || {
                Err(RaftError::ReconfigRejected(
                    "caught-up voter check did not identify a voter".into(),
                ))
            },
            |id| Err(RaftError::VoterNotCaughtUp { id, lag }),
        ),
        VoterReconfigurationDecision::VoterNotFound
        | VoterReconfigurationDecision::DirectoryMismatch => target_id.map_or_else(
            || {
                Err(RaftError::ReconfigRejected(
                    "voter lookup did not identify a voter".into(),
                ))
            },
            |id| Err(RaftError::VoterNotFound(id)),
        ),
        VoterReconfigurationDecision::InvalidVersionTransition => {
            Err(RaftError::InvalidVoterUpdate(format!(
                "kraft.version transition {current_version} -> {requested_version} is not supported"
            )))
        }
        VoterReconfigurationDecision::Admit(_) => Err(RaftError::ReconfigRejected(
            "admitted voter change was handled as a rejection".into(),
        )),
    }
}

/// The KIP-853 controls a leader writes after its `LeaderChange` marker: the
/// `kraft.version` and the voter set when the quorum is at `kraft.version` 1
/// or above and the epoch starts at offset 0, and none otherwise.
///
/// An empty log holds no `VotersRecord`. So when an epoch starts at offset 0,
/// the leader's voters came from the bootstrap checkpoint that
/// `krabka-format --standalone` or `--initial-controllers` wrote. Kafka's
/// `LeaderState.appendStartOfEpochControlRecords` writes that set, the one at
/// offset -1, into the leader's first batch. A replica that never read the
/// checkpoint then learns the voters and their endpoints from the log. That
/// replica is a broker-only observer, or a controller formatted with
/// `--no-initial-controllers`. A later epoch starts past offset 0, on a log
/// that already holds the set.
fn bootstrap_controls(
    kraft_version: u16,
    epoch_start: Offset,
    voters: &VoterSet,
) -> Vec<ControlRecord> {
    if kraft_version == 0 || epoch_start != Offset(0) {
        return Vec::new();
    }
    vec![
        ControlRecord::KRaftVersion(WireKRaftVersionRecord {
            version: 0,
            k_raft_version: i16::try_from(kraft_version).unwrap_or(i16::MAX),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        }),
        ControlRecord::Voters(voter_set_to_wire(voters)),
    ]
}

impl Engine {
    /// Append the batch the leader starts `epoch` with: the `LeaderChange`
    /// control marker, and the bootstrap voter set that
    /// [`bootstrap_controls`] names.
    #[tracing::instrument(level = "info", skip_all, fields(node = self.me.0, epoch), err)]
    pub fn append_leader_change(&mut self, epoch: Epoch) -> Result<Offset, RaftError> {
        let expected_base = self.log.log_end_offset();
        let kraft_version = self.controls.latest_version();
        let voters = self.core.quorum_state().voters.clone();
        let controls = bootstrap_controls(kraft_version, expected_base, &voters);
        let mut batch = start_of_epoch_batch(epoch, self.me, &voters, &controls)?;
        let base = self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())?;
        validate_append_result(
            "leader-change",
            expected_base,
            base,
            self.log.log_end_offset(),
        )?;
        if !controls.is_empty() {
            self.apply_control_batch(&batch)?;
        }
        Ok(base)
    }

    /// What a change names: its kind, the voter it targets, the facts the
    /// decision reads about that voter, and how far the voter's observer
    /// trails the leader.
    fn reconfiguration_target(
        &self,
        change: &VoterChange,
        current: &VoterSet,
        current_version: u16,
        check_only: bool,
    ) -> (VoterChangeKind, Option<NodeId>, TargetVoter, u64) {
        use crate::reconfig::VoterChange;

        match change {
            VoterChange::Add(request) | VoterChange::CheckAdd(request) => {
                let leader_end = self.log.log_end_offset().0;
                // Kafka's `isReplicaCaughtUp` reads the observer state of this
                // exact `(id, directory id)` key. It is the last of the checks,
                // so a check-only request skips it and the range check.
                let progress = self.observers.get(&ReplicaKey {
                    id: request.voter.id,
                    directory_id: request.voter.directory_id,
                });
                let observer_end = progress.map_or(0, |progress| progress.fetch_offset);
                (
                    VoterChangeKind::Add,
                    Some(request.voter.id),
                    TargetVoter {
                        membership: target_membership(
                            current,
                            request.voter.id,
                            request.voter.directory_id,
                        ),
                        version_compatible: check_only
                            || voter_supports_version(&request.voter, current_version),
                        caught_up: check_only
                            || progress.is_some_and(|progress| progress.is_caught_up(self.now())),
                    },
                    u64::try_from(leader_end.saturating_sub(observer_end)).unwrap_or(u64::MAX),
                )
            }
            VoterChange::Remove(request) => (
                VoterChangeKind::Remove,
                Some(request.id),
                // A removal reads only the voter key; the range and catch-up
                // facts are not consulted.
                TargetVoter {
                    membership: target_membership(current, request.id, request.directory_id),
                    version_compatible: true,
                    caught_up: true,
                },
                0,
            ),
            VoterChange::Update(request) => (
                VoterChangeKind::Update,
                Some(request.voter.id),
                TargetVoter {
                    membership: target_membership(
                        current,
                        request.voter.id,
                        request.voter.directory_id,
                    ),
                    version_compatible: voter_supports_version(&request.voter, current_version),
                    // An update is not gated on catch-up.
                    caught_up: true,
                },
                0,
            ),
            // A finalization names no voter; the kernel does not read these.
            VoterChange::FinalizeKraftVersion(_) | VoterChange::ValidateKraftVersion(_) => (
                VoterChangeKind::FinalizeKraftVersion,
                None,
                TargetVoter {
                    membership: TargetMembership::Absent,
                    version_compatible: true,
                    caught_up: true,
                },
                0,
            ),
        }
    }

    /// Validate and append one KIP-853 control operation. The voter set is
    /// applied to the core immediately after append; completion waits for the
    /// resulting batch to cross the HWM unless `AddRaftVoter` v1 requested an
    /// append-only acknowledgement.
    pub fn on_reconfigure(
        &mut self,
        change: VoterChange,
        reply: oneshot::Sender<Result<ReconfigOutcome, RaftError>>,
    ) {
        use crate::reconfig::VoterChange;

        // A check-only add takes the facts about the candidate as given, so it
        // runs the same checks and stops before the append.
        let check_only = matches!(change, VoterChange::CheckAdd(_));
        let current = self.controls.committed_voters.clone();
        let current_version = self.controls.committed_version;
        let leadership = ReconfigurationLeadership {
            is_leader: self.core.role().is_leader(),
            no_pending_change: self.pending_reconfig.is_none(),
            epoch_committed: match self.core.role() {
                Role::Leader {
                    epoch_start_offset, ..
                } => self.log.hwm().0 > *epoch_start_offset,
                _ => false,
            },
        };
        let requested_version = match &change {
            VoterChange::FinalizeKraftVersion(version)
            | VoterChange::ValidateKraftVersion(version) => *version,
            VoterChange::Add(_)
            | VoterChange::CheckAdd(_)
            | VoterChange::Remove(_)
            | VoterChange::Update(_) => current_version,
        };
        let voters = CurrentVoterSet {
            voter_count: current.len(),
            kraft_version: current_version,
            latest_controls_committed: self.controls.latest_voters() == &current
                && self.controls.latest_version() == current_version,
            all_voters_support_requested: current
                .iter()
                .all(|voter| voter_supports_version(voter, requested_version)),
        };
        let (kind, target_id, target, lag) =
            self.reconfiguration_target(&change, &current, current_version, check_only);

        let decision = voter_reconfiguration_decision(
            leadership,
            voters,
            VoterChangeRequest {
                kind,
                requested_kraft_version: requested_version,
            },
            target,
        );
        let plan = match decision {
            VoterReconfigurationDecision::Admit(_) if check_only => {
                let _ = reply.send(Ok(ReconfigOutcome::Committed));
                return;
            }
            VoterReconfigurationDecision::Admit(plan) => plan,
            rejected => {
                let _ = reply.send(rejected_reconfiguration(
                    rejected,
                    self.core.quorum_state().leader_id,
                    target_id,
                    current_version,
                    requested_version,
                    lag,
                ));
                return;
            }
        };

        if matches!(change, VoterChange::ValidateKraftVersion(_)) {
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
            return;
        }

        let (next, ack_when_committed, removed_local_leader) = match change {
            VoterChange::Add(request) | VoterChange::CheckAdd(request) => (
                current.with_voter(request.voter),
                request.ack_when_committed,
                false,
            ),
            VoterChange::Remove(request) => (
                current.without_voter(request.id),
                true,
                request.id == self.me,
            ),
            VoterChange::Update(request) => (current.with_voter(request.voter), true, false),
            VoterChange::FinalizeKraftVersion(_) | VoterChange::ValidateKraftVersion(_) => {
                (current.clone(), true, false)
            }
        };
        if next.len() != plan.next_voter_count {
            let _ = reply.send(Err(RaftError::ReconfigRejected(
                "constructed voter set does not match the proved result".into(),
            )));
            return;
        }

        if plan.preflight_only {
            // At level 0 UpdateVoter supplies upgrade preflight data; no
            // VotersRecord may be written yet.
            self.controls.committed_voters = next.clone();
            self.controls.voter_history.insert(-1, next.clone());
            let actions = self.apply_voter_set(next.clone());
            self.peers.update_voters(&next);
            self.execute(actions);
            self.publish_leader();
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
            return;
        }

        let mut records = Vec::with_capacity(2);
        if plan.write_kraft_version {
            let record = WireKRaftVersionRecord {
                version: 0,
                k_raft_version: i16::try_from(plan.next_kraft_version).unwrap_or(i16::MAX),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            };
            records.push(ControlRecord::KRaftVersion(record));
        }
        if plan.write_voters {
            records.push(ControlRecord::Voters(voter_set_to_wire(&next)));
        }

        let leader_epoch = self.core.quorum_state().leader_epoch;
        let mut batch = match typed_control_batch(leader_epoch, &records) {
            Ok(batch) => batch,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let base = match self
            .log
            .append(&mut batch, KraftController::wall_clock_ms())
        {
            Ok(base) => base,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        if let Err(error) = self.apply_control_batch(&batch) {
            let _ = reply.send(Err(error));
            return;
        }
        let need_offset = Offset(
            base.0
                .saturating_add(i64::try_from(records.len()).unwrap_or(i64::MAX)),
        );
        let waiter_reply = if ack_when_committed {
            Some(reply)
        } else {
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
            None
        };
        self.pending_reconfig = Some(PendingReconfig {
            need_offset,
            reply: waiter_reply,
            removed_local_leader,
        });

        if leader_alone_is_majority(self.core.quorum_state().majority(), self.core.is_voter()) {
            self.advance_and_apply(self.log.log_end_offset());
        }
        self.publish_leader();
    }

    /// Apply KIP-853 controls as soon as their batch is appended or fetched.
    /// Consensus always uses the latest local view, even before commitment.
    pub fn apply_control_batch(&mut self, batch: &RecordBatch) -> Result<(), RaftError> {
        if !batch.attributes.is_control_batch() {
            return Ok(());
        }
        let previous = self.controls.latest_voters().clone();
        for record in &batch.records {
            let control = decode_control_record(record)?;
            let offset = batch
                .base_offset
                .saturating_add(i64::from(record.offset_delta));
            self.controls.apply(offset, &control)?;
        }
        self.apply_changed_voters(&previous);
        Ok(())
    }

    fn apply_changed_voters(&mut self, previous: &VoterSet) {
        let latest = self.controls.latest_voters().clone();
        if latest != *previous {
            let actions = self.apply_voter_set(latest.clone());
            self.peers.update_voters(&latest);
            self.execute(actions);
        }
    }

    /// Release a pending reconfiguration that this leadership can no longer commit.
    pub fn fail_pending_reconfiguration(&mut self) {
        if let Some(mut pending) = self.pending_reconfig.take()
            && let Some(reply) = pending.reply.take()
        {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
        }
    }

    pub fn restore_control_state_after_truncation(&mut self, offset: i64) {
        let previous = self.controls.latest_voters().clone();
        self.controls.truncate_to(offset);
        self.apply_changed_voters(&previous);
        if self
            .pending_reconfig
            .as_ref()
            .is_some_and(|pending| pending.need_offset.0 > offset)
        {
            self.fail_pending_reconfiguration();
        }
    }

    pub fn commit_control_state(&mut self, high_watermark: Offset) {
        if !self.controls.commit_to(high_watermark.0) {
            return;
        }
        self.core.commit_voter_set();
        self.core.set_kraft_version(self.controls.committed_version);
        self.image.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord {
                kraft_version: self.controls.committed_version,
            },
        ));
        self.image.apply(&MetadataRecord::V1Voters(VotersRecord {
            voters: self.controls.committed_voters.clone(),
        }));
        if self.downgrade_snapshot_pending.is_none() {
            let _ = self.image_tx.send(Arc::new(self.image.clone()));
        }
    }

    pub fn try_resolve_reconfiguration(&mut self) {
        let Some(pending) = self.pending_reconfig.as_ref() else {
            return;
        };
        if !hwm_reaches_waiter(self.log.hwm(), pending.need_offset) {
            return;
        }
        let Some(mut pending) = self.pending_reconfig.take() else {
            return;
        };
        if let Some(reply) = pending.reply.take() {
            let _ = reply.send(Ok(ReconfigOutcome::Committed));
        }
        if pending.removed_local_leader {
            let actions = self.core.finish_local_leader_removal(self.now());
            self.execute(actions);
            self.reconcile_timers("leader");
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{KRaftVersionRange, Voter, VoterSet};
    use krabka_verified::reconfiguration::TargetMembership;
    use uuid::Uuid;

    use super::target_membership;
    use crate::NodeId;

    fn voter(id: u64, directory_id: Uuid) -> Voter {
        Voter {
            id: NodeId(id),
            directory_id,
            endpoints: vec![],
            kraft_version: KRaftVersionRange::default(),
        }
    }

    #[test]
    fn a_nil_stored_directory_matches_only_a_nil_request() {
        let current = VoterSet::from_voters([voter(1, Uuid::from_u128(1)), voter(2, Uuid::nil())]);
        let cases = [
            (
                "unknown id",
                3,
                Uuid::from_u128(3),
                TargetMembership::Absent,
            ),
            (
                "same key",
                1,
                Uuid::from_u128(1),
                TargetMembership::PresentSameDirectory,
            ),
            (
                "other directory",
                1,
                Uuid::from_u128(9),
                TargetMembership::PresentOtherDirectory,
            ),
            (
                "nil request",
                1,
                Uuid::nil(),
                TargetMembership::PresentOtherDirectory,
            ),
            (
                "legacy voter",
                2,
                Uuid::from_u128(2),
                TargetMembership::PresentUnknownDirectory,
            ),
            (
                "legacy nil request",
                2,
                Uuid::nil(),
                TargetMembership::PresentSameDirectory,
            ),
        ];
        for (case, id, directory_id, expected) in cases {
            assert!(
                target_membership(&current, NodeId(id), directory_id) == expected,
                "{case}"
            );
        }
    }
}
