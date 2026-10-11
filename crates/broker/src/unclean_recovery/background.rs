//! KFC-9: the break-glass rule the URM applies to a recovery nobody asked for.
//!
//! The rule reads `[break_glass]` once and then answers two questions per job:
//! whether the recovery may run at all, and whether the election that followed
//! has to be recorded as a bypass. Both answers, and the audit events that
//! carry them, live here so the manager keeps only the control flow.

use std::sync::Arc;

use krabka_audit::{
    AuditEndpoint, AuditError, AuditEvent, AuditLog, AuditMode, AuditOutcome, AuditPrincipal,
    PrivilegedPhase,
};
use krabka_metadata::BreakGlassAction;
use krabka_raft::NodeId;

use super::{Election, RecoveryJob};
use crate::{
    break_glass::{action_name, gate::is_gated, metrics as break_glass_metrics},
    config::{BackgroundUncleanRecovery, BreakGlassConfig},
    metrics::BrokerMetrics,
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
};

/// KFC-9: the break-glass rule for a recovery that nobody asked for.
///
/// # This path has no caller to refuse
///
/// Unclean recovery can lose committed data exactly as an operator-typed
/// unclean election does -- whenever no eligible leader replica survives to be
/// elected instead -- so the two-person rule looks as if it belongs on both.
/// It cannot be here. Leader election and the broker-heartbeat path start a
/// recovery with no request, no connection, and no principal, so a refusal has
/// no recipient and an approval has nobody to ask. An operator who types an
/// unclean election can be asked for a second signature, and a controller that
/// reacts to a dead broker at 03:00 cannot.
///
/// [`BackgroundUncleanRecovery`] is the three-valued answer to that, and
/// `audit-only` is the default for the reason the split states.
/// [`BackgroundUncleanRecovery::Require`] is the fail-closed option, and it
/// costs every partition whose leader dies at 03:00 its availability, and not
/// only the ones an incident touches.
///
/// A job that carries a proposal took the operator path, where the handler
/// already spent an approved proposal. None of this applies to it.
#[derive(Debug, Clone)]
pub(crate) struct BackgroundRecovery {
    /// Whether this broker runs the two-person rule at all.
    ///
    /// An empty `break_glass.approvers` turns the workflow off, and nobody can
    /// approve anything. Every recovery would then be unapproved, so `require`
    /// would make unclean recovery impossible and `audit-only` would count
    /// every failover as a bypass of a rule that does not exist. A cluster with
    /// no `[break_glass]` section behaves exactly as it does today, which is
    /// the rule the whole feature follows.
    enabled: bool,
    /// The configured `break_glass.background_unclean_recovery`.
    mode: BackgroundUncleanRecovery,
    /// Where the bypass and the refusal events go.
    audit_log: Arc<AuditLog>,
    /// A fingerprint of the sorted approver set, as every other break-glass
    /// event carries. Two brokers that disagree about `break_glass.approvers`
    /// are then visible after the fact.
    approver_set_fingerprint: String,
}

impl BackgroundRecovery {
    /// Read the rule out of `[break_glass]`.
    pub(crate) fn new(config: &BreakGlassConfig, audit_log: Arc<AuditLog>) -> Self {
        Self {
            enabled: is_gated(config),
            mode: config.background_unclean_recovery,
            audit_log,
            approver_set_fingerprint: approver_set_fingerprint(&config.approvers),
        }
    }

    /// Whether this job must not run at all.
    ///
    /// Only [`BackgroundUncleanRecovery::Require`] refuses, and only a job that
    /// no operator approved on a broker that runs the two-person rule.
    ///
    /// The manager asks only about a recovery that can lose data. A partition
    /// that still has a surviving eligible leader replica can be recovered
    /// without losing one, and this rule has no reason to refuse that.
    pub(super) fn refuses(&self, job: &RecoveryJob) -> bool {
        self.enabled && job.proposal.is_none() && self.mode == BackgroundUncleanRecovery::Require
    }

    /// Whether this job must not commit the election the poll settled on.
    ///
    /// The same rule as [`Self::refuses`], asked once the poll has said which
    /// of the two elections the recovery reached. An eligible leader replica
    /// holds every committed record, so electing one is not the act the rule
    /// exists to stop and `require` lets it through; a fallback to the most
    /// complete surviving log is that act, and `require` refuses it however
    /// the partition's published ELR read before the poll.
    pub(super) fn refuses_election(&self, job: &RecoveryJob, election: Election) -> bool {
        election.basis.loses_data() && self.refuses(job)
    }

    /// Audit a recovery this rule refused. The partition stays leaderless and
    /// visibly offline, so the audit log is the only place that says why.
    pub(super) fn audit_refusal(&self, job: &RecoveryJob, node_id: NodeId) {
        self.emit(
            PrivilegedPhase::Refused,
            AuditOutcome::Failure,
            job,
            node_id,
            format!(
                "break_glass.background_unclean_recovery is require, and no proposal approved \
                 this recovery; the partition stays offline (strategy {:?})",
                job.strategy
            ),
        );
    }

    /// Record one committed election, and account it as a bypass when it was
    /// one.
    ///
    /// A broker that runs the rule writes an event for every recovery that
    /// reaches raft, and its reason names which rule chose the leader: an
    /// eligible leader replica, or the most complete surviving log. The two
    /// are worth telling apart after the fact, because only the second one can
    /// have dropped a committed record.
    ///
    /// A data-losing election that no operator approved is also a bypass of
    /// the two-person rule, and is recorded as one. `audit-only` is the
    /// default, so that is the ordinary path on a cluster that turned the rule
    /// on: the counter is the series to alert on, and the event is the
    /// after-the-fact proof that a data-losing election happened that no
    /// second person agreed to. An ELR election is not a bypass -- it loses
    /// nothing, so there is nothing the rule would have refused.
    ///
    /// `off`, and a broker with no approver set, write nothing at all. KFC-9
    /// gives `off` one meaning, the behaviour the broker had before the rule
    /// existed with no audit event and no counter, and an `applied` event per
    /// recovery would take it away.
    pub(super) fn audit_election(
        &self,
        job: &RecoveryJob,
        node_id: NodeId,
        election: Election,
        metrics: &BrokerMetrics,
    ) {
        if !self.enabled || self.mode == BackgroundUncleanRecovery::Off {
            return;
        }
        let elected = format!("unclean recovery elected {}", Self::choice(election, job));
        if !self.is_bypass(job, election) {
            self.emit(
                PrivilegedPhase::Applied,
                AuditOutcome::Success,
                job,
                node_id,
                elected,
            );
            return;
        }
        break_glass_metrics::record_bypass(metrics, BreakGlassAction::UncleanRecovery);
        self.emit(
            PrivilegedPhase::Bypassed,
            AuditOutcome::Success,
            job,
            node_id,
            format!("{elected}, with no break-glass approval"),
        );
    }

    /// Whether this election bypassed a two-person rule that was watching for
    /// it. Only a data-losing one can: an ELR election is what the rule would
    /// have allowed.
    fn is_bypass(&self, job: &RecoveryJob, election: Election) -> bool {
        self.enabled
            && job.proposal.is_none()
            && self.mode == BackgroundUncleanRecovery::AuditOnly
            && election.basis.loses_data()
    }

    /// The clause naming the leader and the rule that chose it, which every
    /// event on this path is built from.
    fn choice(election: Election, job: &RecoveryJob) -> String {
        format!(
            "broker {} {} (strategy {:?})",
            election.leader.0,
            election.basis.describe(),
            job.strategy
        )
    }

    /// Durably admit the election before it reaches raft.
    pub(super) async fn require_audit(
        &self,
        job: &RecoveryJob,
        node_id: NodeId,
        election: Election,
    ) -> Result<(), AuditError> {
        if self.audit_log.mode() != AuditMode::FailClosed {
            return Ok(());
        }
        let reason = format!(
            "unclean recovery admitted for {}",
            Self::choice(election, job)
        );
        self.audit_log
            .emit_required(self.event(
                PrivilegedPhase::Attempted,
                AuditOutcome::Success,
                job,
                node_id,
                reason,
            ))
            .await
    }

    /// Emit one `PrivilegedAction` event for this path.
    ///
    /// The event names the controller that acted rather than a person, and its
    /// source endpoint is empty, because no connection carried this recovery.
    /// That absence is the whole reason the path cannot have a gate.
    ///
    /// It does carry the proposal id when the job has one. An operator-typed
    /// recovery reaches the URM with the approval its handler already spent,
    /// and the applied event is the one that records the election as
    /// authorized rather than a bypass; without the id, nothing joins that
    /// record to the approval that authorized it. A background job has no
    /// proposal and the field stays empty, which is what a refusal always is.
    fn emit(
        &self,
        phase: PrivilegedPhase,
        outcome: AuditOutcome,
        job: &RecoveryJob,
        node_id: NodeId,
        reason: String,
    ) {
        self.audit_log
            .emit(self.event(phase, outcome, job, node_id, reason));
    }

    fn event(
        &self,
        phase: PrivilegedPhase,
        outcome: AuditOutcome,
        job: &RecoveryJob,
        node_id: NodeId,
        reason: String,
    ) -> AuditEvent {
        AuditEvent::PrivilegedAction {
            outcome,
            phase,
            action: action_name(BreakGlassAction::UncleanRecovery).to_owned(),
            target: format!("{}-{}", job.topic, job.partition),
            proposal_id: job.proposal.map(|id| id.to_string()).unwrap_or_default(),
            principal: AuditPrincipal {
                name: format!("Controller:{}", node_id.0),
                auth_method: "Internal".to_owned(),
            },
            counterparties: Vec::new(),
            approver_set_fingerprint: self.approver_set_fingerprint.clone(),
            key_id: String::new(),
            signature: Vec::new(),
            signature_verified: false,
            // The controller runs this with no caller and no signature.
            signed_at_ms: 0,
            source: AuditEndpoint {
                ip: String::new(),
                port: 0,
            },
            reason,
            time_ms: now_ms(),
        }
    }
}
