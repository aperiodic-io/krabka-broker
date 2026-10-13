//! The `PrivilegedAction` event that a gated transition emits.
//!
//! Five handlers gate a privileged transition — an unclean election, an
//! unregistration, a reassignment cancel, a topic deletion, and a record trim
//! — and every one of them records the same event in the same shape. One
//! helper is what keeps a new phase, a new field, or a corrected guard from
//! reaching four of the five.

use {
    crate::{
        break_glass::{
            action_name, gate,
            handlers::{PrivilegedAudit, audit_privileged, require_privileged},
        },
        config::BreakGlassConfig,
        handlers::RequestContext,
        operator_keys::approver_set_fingerprint,
    },
    krabka_audit::{AuditError, AuditLog, AuditOutcome, PrivilegedPhase},
    krabka_metadata::BreakGlassAction,
    uuid::Uuid,
};

/// One gated transition, as its audit event records it.
///
/// The struct exists so that each call site names the field it fills, which is
/// the reason [`PrivilegedAudit`] is a struct too.
pub(crate) struct GatedTransition<'a> {
    /// The gated transition this event is about.
    pub action: BreakGlassAction,
    /// What the transition applies to, in the spelling the gate resolves.
    pub target: &'a str,
    /// Which step of the workflow this event records.
    pub phase: PrivilegedPhase,
    /// The proposal that authorized the transition, or `None` when none did.
    pub proposal_id: Option<Uuid>,
    /// Free text that says what happened, or why it did not.
    pub reason: &'a str,
}

/// Durably admit a gated transition before it mutates cluster state.
pub(crate) async fn require_transition(
    audit_log: &AuditLog,
    config: &BreakGlassConfig,
    ctx: &RequestContext<'_>,
    transition: &GatedTransition<'_>,
) -> Result<(), AuditError> {
    if !gate::is_gated(config) {
        return Ok(());
    }
    require_privileged(
        audit_log,
        ctx,
        approver_set_fingerprint(&config.approvers),
        &transition_audit(transition, AuditOutcome::Success),
    )
    .await
}

/// Emit one `PrivilegedAction` event for a gated transition.
///
/// A broker whose `[break_glass]` names no approver emits nothing: it gates
/// nothing, so it has no two-person evidence to record, and this event exists
/// to carry that evidence. The ordinary administrative event already reports
/// the transition itself, so a stock cluster's audit stream is unchanged by
/// the feature.
///
/// `counterparties` stays empty for the reason the freeze events give: the
/// approvers are named on the proposal's own approve events, and the proposal
/// id joins those rows to this one.
///
/// An `Applied` event belongs after the batch append commits. Emitting one
/// before the commit would record a transition that the quorum can still
/// refuse.
pub(crate) fn audit_transition(
    audit_log: &AuditLog,
    config: &BreakGlassConfig,
    ctx: &RequestContext<'_>,
    transition: &GatedTransition<'_>,
) {
    if !gate::is_gated(config) {
        return;
    }
    audit_privileged(
        audit_log,
        ctx,
        approver_set_fingerprint(&config.approvers),
        &transition_audit(
            transition,
            if matches!(transition.phase, PrivilegedPhase::Refused) {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            },
        ),
    );
}

/// The [`PrivilegedAudit`] that `transition` records with `outcome`.
///
/// A gated transition is never signed and names no counterparty: the
/// approvers are named on the proposal's own approve events.
fn transition_audit<'a>(
    transition: &GatedTransition<'a>,
    outcome: AuditOutcome,
) -> PrivilegedAudit<'a> {
    PrivilegedAudit {
        outcome,
        phase: transition.phase,
        action: action_name(transition.action),
        target: transition.target,
        proposal_id: transition.proposal_id,
        counterparties: &[],
        key_id: "",
        signature: &[],
        signature_verified: false,
        reason: transition.reason,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            break_glass::{
                ALL_ACTIONS,
                handlers::tests::{audit_channel, context, peer, principal},
            },
            broker::Broker,
        },
        assert2::{assert, check},
        krabka_audit::{
            AuditEndpoint, AuditEvent, AuditMode, AuditPrincipal, AuditRecord, AuditSink,
            AuditStats, AuditWriter, AuditWriterParams, ChainState, LifecycleKind, Spool,
        },
        krabka_units::prelude::{hours, mebibytes},
        std::sync::Arc,
    };

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

    fn gated_config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..BreakGlassConfig::default()
        }
    }

    fn trim() -> GatedTransition<'static> {
        GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: "orders-3",
            phase: PrivilegedPhase::Applied,
            proposal_id: Some(PROPOSAL),
            reason: "records deleted below the trim point",
        }
    }

    #[derive(Debug)]
    struct FailingSink;

    #[async_trait::async_trait]
    impl AuditSink for FailingSink {
        async fn write(&self, _record: AuditRecord, _durable: bool) -> Result<(), AuditError> {
            Err(AuditError::Sink("offline".into()))
        }
    }

    fn full_spool() -> (tempfile::TempDir, Spool) {
        let existing = AuditRecord::from_event(
            &AuditEvent::Lifecycle {
                kind: LifecycleKind::BrokerStarted,
                node_id: 1,
                time_ms: 1,
            },
            &Broker::audit_product(),
        );
        let probe = tempfile::tempdir().expect("probe tempdir");
        let one = {
            let mut spool = Spool::open(probe.path(), mebibytes(1)).expect("open probe spool");
            check!(spool.append(&existing).expect("append probe record"));
            spool.size()
        };

        let dir = tempfile::tempdir().expect("spool tempdir");
        let mut spool = Spool::open(dir.path(), one).expect("open exact spool");
        check!(spool.append(&existing).expect("fill spool"));
        (dir, spool)
    }

    async fn saturated_transition(mode: AuditMode) -> (Result<(), AuditError>, u64) {
        let (_dir, spool) = full_spool();
        let stats = Arc::new(AuditStats::new());
        let (log, receiver) = AuditLog::new_with_mode_and_spool(8, mode, &spool);
        let writer = AuditWriter::new(receiver, AuditWriterParams {
            sink: Arc::new(FailingSink),
            product: Broker::audit_product(),
            signer: None,
            checkpoint_every_n: 0,
            checkpoint_every: hours(1),
            chain: ChainState::new(),
            spool: Some(spool),
            stats: Arc::clone(&stats),
            replay_every: hours(1),
            timer: Arc::new(qubit_clock::StdTimer::new()),
        });
        let writer = tokio::spawn(writer.run());
        let principal = principal("alice");
        let peer = peer();
        let result =
            require_transition(&log, &gated_config(), &context(&principal, &peer), &trim()).await;
        if result.is_ok() {
            audit_transition(&log, &gated_config(), &context(&principal, &peer), &trim());
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while stats.dropped() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("audit loss is counted");
        log.close();
        writer.await.expect("audit writer stops");
        (result, stats.dropped())
    }

    #[tokio::test]
    async fn saturated_audit_spool_applies_fail_open_and_refuses_fail_closed() {
        let (open, open_dropped) = saturated_transition(AuditMode::FailOpen).await;
        check!(open.is_ok(), "fail-open transition is applied");
        check!(open_dropped == 1, "fail-open audit loss is counted");

        let (closed, closed_dropped) = saturated_transition(AuditMode::FailClosed).await;
        assert!(let Err(error) = closed);
        check!(error.to_string().contains("spool is full"));
        check!(closed_dropped == 1, "fail-closed refusal is counted");
    }

    #[test]
    fn a_broker_that_names_no_approver_emits_no_privileged_event() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        // Every phase is silent, not just the refusals: with no rule there is
        // no two-person evidence for any of them to carry.
        for phase in [
            PrivilegedPhase::Applied,
            PrivilegedPhase::Refused,
            PrivilegedPhase::Consumed,
        ] {
            audit_transition(
                &audit_log,
                &BreakGlassConfig::default(),
                &context(&principal, &peer),
                &GatedTransition { phase, ..trim() },
            );
        }

        check!(events.try_recv().is_err());
    }

    #[test]
    fn a_gated_transition_emits_the_event_the_two_person_rule_needs() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();
        let config = gated_config();

        audit_transition(&audit_log, &config, &context(&principal, &peer), &trim());

        assert!(let Ok(event) = events.try_recv());
        let AuditEvent::PrivilegedAction { time_ms, .. } = &event else {
            panic!("the helper emits a privileged-action event")
        };
        let expected = AuditEvent::PrivilegedAction {
            outcome: AuditOutcome::Success,
            phase: PrivilegedPhase::Applied,
            action: "delete_records".to_owned(),
            target: "orders-3".to_owned(),
            proposal_id: PROPOSAL.to_string(),
            principal: AuditPrincipal {
                name: "User:alice".to_owned(),
                auth_method: "SaslPlain".to_owned(),
            },
            counterparties: Vec::new(),
            approver_set_fingerprint: approver_set_fingerprint(&config.approvers),
            key_id: String::new(),
            signature: Vec::new(),
            signature_verified: false,
            signed_at_ms: 0,
            source: AuditEndpoint {
                ip: "10.0.0.7".to_owned(),
                port: 51120,
            },
            reason: "records deleted below the trim point".to_owned(),
            time_ms: *time_ms,
        };
        check!(event == expected);
    }

    #[test]
    fn a_refusal_is_the_one_phase_that_records_a_failure() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        for (label, phase, expected) in [
            ("a refusal", PrivilegedPhase::Refused, AuditOutcome::Failure),
            (
                "a consume",
                PrivilegedPhase::Consumed,
                AuditOutcome::Success,
            ),
            (
                "an application",
                PrivilegedPhase::Applied,
                AuditOutcome::Success,
            ),
        ] {
            audit_transition(
                &audit_log,
                &gated_config(),
                &context(&principal, &peer),
                &GatedTransition { phase, ..trim() },
            );

            assert!(let Ok(event) = events.try_recv(), "case {label}");
            let AuditEvent::PrivilegedAction { outcome, .. } = &event else {
                panic!("case {label}: the helper emits a privileged-action event")
            };
            check!(*outcome == expected, "case {label}");
        }
    }

    #[test]
    fn every_action_reaches_the_event_under_its_one_name() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        for action in ALL_ACTIONS {
            audit_transition(
                &audit_log,
                &gated_config(),
                &context(&principal, &peer),
                &GatedTransition { action, ..trim() },
            );

            assert!(let Ok(event) = events.try_recv(), "{action:?}");
            let AuditEvent::PrivilegedAction { action: named, .. } = &event else {
                panic!("the helper emits a privileged-action event")
            };
            check!(named.as_str() == action_name(action));
        }
    }
}
