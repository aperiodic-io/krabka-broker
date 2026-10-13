//! The raft append a gated metadata transition accumulates, and the audit trail
//! that append owes once its outcome is known.
//!
//! `ElectLeaders` and `AlterPartitionReassignments` both gate a per-partition
//! transition on a break-glass approval and both commit every consumed proposal
//! in the same raft append as the partition records it authorized. They differ
//! only in the action they spend and the wording of their audit events, so
//! [`GatedBatch`] takes those as data.

use {
    crate::{
        break_glass::{
            gate::consumed_proposal_id,
            handlers::audit::{GatedTransition, audit_transition, require_transition},
        },
        broker::Broker,
        handlers::RequestContext,
    },
    krabka_audit::{AuditError, PrivilegedPhase},
    krabka_metadata::{BreakGlassAction, MetadataRecord},
    std::collections::HashSet,
    uuid::Uuid,
};

/// What one gated request accumulates across its partitions.
///
/// `records` is the single raft append that carries every consumed proposal
/// beside every partition record the request makes. That one append is why a
/// proposal lives in the metadata log at all: the approval and the transition
/// it authorizes commit together, so a crash between them cannot spend one
/// approval twice.
pub(crate) struct GatedBatch {
    /// The gated transition every queued event records.
    action: BreakGlassAction,
    /// The `Applied` reason of the event that durably admits a transition
    /// before the append.
    admitted_reason: &'static str,
    /// The `Applied` reason of the event that records a committed append.
    committed_reason: &'static str,
    /// The consumed proposals first, then the partition records.
    pub(crate) records: Vec<MetadataRecord>,
    /// The proposals this request already spent. One approved proposal on a
    /// bare topic name covers every partition of that topic, so a request that
    /// touches ten of them reads one proposal ten times and spends it once.
    pub(crate) spent: HashSet<Uuid>,
    /// The transitions waiting on the append, each as its target and the
    /// proposal that authorized it, to audit once the append's outcome is known.
    pub(crate) applied: Vec<(String, Option<Uuid>)>,
}

/// The phase and reason of the event that records an append's outcome.
#[derive(Debug, PartialEq)]
struct AppendOutcome<'a> {
    phase: PrivilegedPhase,
    reason: &'a str,
}

impl GatedBatch {
    /// An empty batch for `action`, whose `Applied` events carry
    /// `admitted_reason` before the append and `committed_reason` after it.
    pub(crate) fn new(
        action: BreakGlassAction,
        admitted_reason: &'static str,
        committed_reason: &'static str,
    ) -> Self {
        Self {
            action,
            admitted_reason,
            committed_reason,
            records: Vec::new(),
            spent: HashSet::new(),
            applied: Vec::new(),
        }
    }

    /// Take a consumed proposal into the append, and answer the proposal it
    /// names.
    ///
    /// The record goes in ahead of every partition record, and only the first
    /// time this request sees the proposal.
    pub(crate) fn spend(&mut self, consumed: Option<MetadataRecord>) -> Option<Uuid> {
        let consumed = consumed?;
        let proposal_id = consumed_proposal_id(&consumed)?;
        if self.spent.insert(proposal_id) {
            self.records.insert(0, consumed);
        }
        Some(proposal_id)
    }

    /// Durably admit every queued transition before the raft append.
    pub(crate) async fn require_audit(
        &self,
        broker: &Broker,
        ctx: &RequestContext<'_>,
    ) -> Result<(), AuditError> {
        for (target, proposal_id) in &self.applied {
            require_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: self.action,
                    target,
                    phase: PrivilegedPhase::Applied,
                    proposal_id: *proposal_id,
                    reason: self.admitted_reason,
                },
            )
            .await?;
        }
        Ok(())
    }

    /// The phase and reason of the event that records the append's outcome:
    /// the committed reason when `failure` is `None`, and a refusal carrying
    /// the submit error otherwise.
    fn outcome<'a>(&self, failure: Option<&'a str>) -> AppendOutcome<'a> {
        match failure {
            None => AppendOutcome {
                phase: PrivilegedPhase::Applied,
                reason: self.committed_reason,
            },
            Some(error) => AppendOutcome {
                phase: PrivilegedPhase::Refused,
                reason: error,
            },
        }
    }

    /// Audit every transition this append carried.
    ///
    /// `failure` is the submit error when the append did not commit, and the
    /// event then records a refusal with that text rather than a transition
    /// that never happened.
    pub(crate) fn audit_applied(
        &self,
        broker: &Broker,
        ctx: &RequestContext<'_>,
        failure: Option<&str>,
    ) {
        let AppendOutcome { phase, reason } = self.outcome(failure);
        for (target, proposal_id) in &self.applied {
            audit_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: self.action,
                    target,
                    phase,
                    proposal_id: *proposal_id,
                    reason,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn an_append_that_did_not_commit_is_audited_as_a_refusal() {
        let batch = GatedBatch::new(
            BreakGlassAction::UncleanElectLeaders,
            "admitted before the append",
            "committed in the append",
        );

        for (failure, expected) in [
            (None, AppendOutcome {
                phase: PrivilegedPhase::Applied,
                reason: "committed in the append",
            }),
            (Some("submit failed: not the controller"), AppendOutcome {
                phase: PrivilegedPhase::Refused,
                reason: "submit failed: not the controller",
            }),
        ] {
            assert!(batch.outcome(failure) == expected, "{failure:?}");
        }
    }
}
