//! KFC-9: the break-glass approval a reassignment cancel needs, the per-row
//! step that spends it, and the batch one request accumulates.
//!
//! [`alter_one`] is the whole of one requested row: it resolves the gate for a
//! cancel, hands the row to the pure planner in [`plan`](super::plan), and
//! answers with the response row that row becomes. A start never reaches the
//! gate, because only a cancel is gated.
//!
//! [`ReassignBatch`] is what carries a consumed approval into the same raft
//! append as the partition record it authorized, so the two commit together,
//! and it holds back the `Applied` audit events until that append's outcome is
//! known.

use {
    super::{
        process_one_partition,
        response::{err_row, ok_row},
    },
    crate::{
        break_glass::handlers::batch::GatedBatch,
        broker::Broker,
        codes::POLICY_VIOLATION,
        freeze::resolve::{FreezeMutationResolution, FreezeVerdict},
        handlers::{RequestContext, partition_transition::audit_refusal},
    },
    krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        alter_partition_reassignments_request::ReassignablePartition,
        alter_partition_reassignments_response::ReassignablePartitionResponse,
    },
    std::{
        collections::HashSet,
        ops::{Deref, DerefMut},
    },
};

/// Everything one alter row reads, and nothing it writes.
pub(super) struct ReassignEnv<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    pub(super) allow_rf_change: bool,
}

/// What one `AlterPartitionReassignments` request accumulates across its rows.
///
/// The [`GatedBatch`] it derefs to carries every consumed proposal beside every
/// partition record the request makes in one raft append, so an approval and
/// the cancel it authorized commit together.
pub(super) struct ReassignBatch {
    /// The append, the spent proposals, and the cancels waiting on its audit.
    gated: GatedBatch,
    /// The partitions that contributed a record to the append, in the
    /// `"<topic>-<partition>"` spelling the audit resource carries. A row
    /// already at its requested target plans no record and belongs in no audit
    /// event, however successful its response row is.
    pub(super) altered: HashSet<String>,
}

impl Default for ReassignBatch {
    fn default() -> Self {
        Self {
            gated: GatedBatch::new(
                BreakGlassAction::CancelReassignment,
                "reassignment cancel admitted",
                "reassignment cancel committed",
            ),
            altered: HashSet::new(),
        }
    }
}

impl Deref for ReassignBatch {
    type Target = GatedBatch;

    fn deref(&self) -> &GatedBatch { &self.gated }
}

impl DerefMut for ReassignBatch {
    fn deref_mut(&mut self) -> &mut GatedBatch { &mut self.gated }
}

/// Process one alter row, and answer the response row it becomes.
pub(super) fn alter_one(
    env: &ReassignEnv<'_>,
    batch: &mut ReassignBatch,
    topic: &str,
    partition: &ReassignablePartition,
    freeze: FreezeMutationResolution<'_>,
) -> ReassignablePartitionResponse {
    let index = partition.partition_index;
    if let FreezeMutationResolution::Frozen(record) = freeze {
        return err_row(
            index,
            POLICY_VIOLATION,
            FreezeVerdict::from(record).reassignment_message(),
        );
    }
    let target: Option<&[i32]> = partition.replicas.as_deref();
    // KFC-9: only a cancel is gated. A start adds replicas and removes none,
    // and a completion is not a cancel at all.
    let mut consumed = None;
    let mut denial = None;
    if target.is_none() {
        match authorize_cancel(env.image, &env.broker.config.break_glass, topic, index) {
            Ok(record) => consumed = record,
            Err(refusal) => denial = Some(refusal),
        }
    }

    match process_one_partition(
        env.image,
        topic,
        index,
        target,
        env.allow_rf_change,
        denial.is_none(),
    ) {
        Ok(Some(record)) => {
            let proposal_id = batch.spend(consumed);
            batch.records.push(MetadataRecord::V1Partition(record));
            batch.altered.insert(cancel_target(topic, index));
            if target.is_none() {
                batch
                    .applied
                    .push((cancel_target(topic, index), proposal_id));
            }
            ok_row(index)
        }
        Ok(None) => ok_row(index),
        Err((code, message)) => {
            // The pure function knows only that the cancel is unapproved. The
            // gate's own text names the proposal that nearly authorized it, so
            // that is what the row and the audit event carry.
            let Some(denial) = denial.filter(|_| code == POLICY_VIOLATION) else {
                return err_row(index, code, message);
            };
            let message = denial.to_string();
            audit_refusal(
                env.broker,
                env.ctx,
                BreakGlassAction::CancelReassignment,
                || cancel_target(topic, index),
                &denial,
                &message,
            );
            err_row(index, code, message)
        }
    }
}

crate::handlers::partition_transition::authorizer! {
/// KFC-9: find the approved proposal that authorizes a cancel of one partition,
/// and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. A cancel then behaves as it does on a cluster with no such
/// section.
fn authorize_cancel = CancelReassignment;
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
use crate::handlers::partition_transition::partition_target as cancel_target;

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            break_glass::gate::tests::{APPROVED_PROPOSAL_ID, approved_proposal},
            config::BreakGlassConfig,
            handlers::alter_partition_reassignments::test_support::{
                ReassignmentImageSetup, img_with, pending_reassignment_image,
            },
            test_support::{ReassignmentSetup, start_broker_no_audit_with},
        },
        assert2::{assert, check},
        std::sync::Arc,
        uuid::Uuid,
    };

    const NOW_MS: i64 = 60_000;

    macro_rules! gated_cancel_fixture {
        (($handle:ident, $directory:ident, $broker:ident)) => {
            broker_fixture!(
                ($handle, $directory, $broker),
                crate::test_support::start_broker_no_audit_with(|config| {
                    config.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
                    config.break_glass = gated_config();
                })
            );
        };
    }

    fn gated_config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..BreakGlassConfig::default()
        }
    }

    /// A partition mid-reassignment, beside the proposals the registry holds.
    fn img_reassigning(proposals: &[krabka_metadata::BreakGlassProposalRecord]) -> MetadataImage {
        let mut img = pending_reassignment_image();
        for proposal in proposals {
            img.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        img
    }

    #[test]
    fn the_cancel_gate_answers_from_the_proposal_registry() {
        let approved = approved_proposal(BreakGlassAction::CancelReassignment, "foo-0");
        let cases: [(&'static str, MetadataImage, BreakGlassConfig, bool); 4] = [
            (
                "an approved proposal on the partition",
                img_reassigning(std::slice::from_ref(&approved)),
                gated_config(),
                true,
            ),
            (
                "an approved proposal on the whole topic",
                img_reassigning(&[approved_proposal(
                    BreakGlassAction::CancelReassignment,
                    "foo",
                )]),
                gated_config(),
                true,
            ),
            (
                "no proposal at all",
                img_reassigning(&[]),
                gated_config(),
                false,
            ),
            (
                "no approver set, so nothing is gated",
                img_reassigning(&[]),
                BreakGlassConfig::default(),
                true,
            ),
        ];
        for (label, img, config, expected) in cases {
            let authorized = authorize_cancel(&img, &config, "foo", 0).is_ok();
            check!(authorized == expected, "case {label}");
        }
    }

    macro_rules! reassign_env_fixture {
        (($principal:ident, $peer:ident, $ctx:ident, $env:ident), $broker:ident, $image:ident) => {
            request_identity!(
                ($principal, $peer, $ctx),
                crate::test_support::principal("admin"),
                client_id = "reassign-client",
                address = crate::test_support::peer()
            );
            let $env = ReassignEnv {
                broker: &$broker,
                image: &$image,
                ctx: &$ctx,
                allow_rf_change: true,
            };
        };
    }

    #[tokio::test]
    async fn an_approved_cancel_appends_the_consume_beside_the_partition_record() {
        gated_cancel_fixture!((handle, _dir, broker));
        let proposal = approved_proposal(BreakGlassAction::CancelReassignment, "foo-0");
        let image = img_reassigning(std::slice::from_ref(&proposal));
        let (row, batch) = cancel(&broker, &image);

        check!(row.error_code == 0);
        // The consume and the cancel it authorized are one raft append.
        assert!(batch.records.len() == 2, "{:?}", batch.records);
        assert!(let MetadataRecord::V1BreakGlassProposal(consumed) = &batch.records[0]);
        check!(consumed.proposal_id == APPROVED_PROPOSAL_ID);
        check!(consumed.consumed_at_ms != 0, "the approval is spent");
        let reverted = process_one_partition(&image, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");
        check!(batch.records[1] == MetadataRecord::V1Partition(reverted));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn an_unapproved_cancel_appends_nothing_and_carries_the_gate_text() {
        gated_cancel_fixture!((handle, _dir, broker));
        let image = img_reassigning(&[]);
        let (row, batch) = cancel(&broker, &image);

        check!(row.error_code == POLICY_VIOLATION);
        check!(
            row.error_message
                == Some(
                    "break-glass refused cancel_reassignment on foo-0: no approved proposal covers the request"
                        .to_owned()
                )
        );
        assert!(batch.records == vec![], "a refused cancel appends nothing");
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn a_freeze_refuses_start_and_cancel_before_any_record_or_approval_spend() {
        gated_cancel_fixture!((handle, _dir, broker));
        let image = img_reassigning(&[approved_proposal(
            BreakGlassAction::CancelReassignment,
            "foo-0",
        )]);
        reassign_env_fixture!((principal, peer, ctx, env), broker, image);
        let record = krabka_metadata::TopicFreezeRecord {
            scope: "foo".into(),
            pattern_type: krabka_metadata::PatternType::Literal,
            frozen: true,
            reason: "DR cutover".into(),
            set_by: "User:alice".into(),
            set_at_ms: 10,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        };

        for (label, replicas) in [("start", Some(vec![1, 3])), ("cancel", None)] {
            let mut batch = ReassignBatch::default();
            let row = alter_one(
                &env,
                &mut batch,
                "foo",
                &ReassignablePartition {
                    partition_index: 0,
                    replicas,
                    ..Default::default()
                },
                FreezeMutationResolution::Frozen(&record),
            );

            check!(row.error_code == POLICY_VIOLATION, "{label}");
            check!(
                row.error_message
                    == Some(
                        "a write freeze on the literal scope \"foo\" refuses this reassignment: DR cutover"
                            .to_owned()
                    ),
                "{label}"
            );
            assert!(batch.records.is_empty(), "{label} must append nothing");
        }
        handle.shutdown().await;
    }

    /// A start whose target is already the partition's target plans no record,
    /// and its successful row must not enter the audited set: an audit trail
    /// that names it claims a reassignment that never happened.
    #[tokio::test]
    async fn only_a_start_that_plans_a_record_counts_as_altered() {
        let (handle, _dir) = start_broker_no_audit_with(|cfg| {
            cfg.authorizer = Arc::new(AllowAllAuthorizer);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let image = img_with(ReassignmentImageSetup {
            assignment: ReassignmentSetup {
                replicas: vec![krabka_ids::NodeId(1), krabka_ids::NodeId(2)],
                isr: vec![krabka_ids::NodeId(1), krabka_ids::NodeId(2)],
                ..Default::default()
            },
            ..Default::default()
        });
        reassign_env_fixture!((principal, peer, ctx, env), broker, image);

        for (label, replicas, altered) in [
            ("already at the requested target", vec![1, 2], false),
            ("a target that moves a replica", vec![1, 3], true),
        ] {
            let mut batch = ReassignBatch::default();

            let row = alter_one(
                &env,
                &mut batch,
                "foo",
                &ReassignablePartition {
                    partition_index: 0,
                    replicas: Some(replicas),
                    ..Default::default()
                },
                FreezeMutationResolution::Admit,
            );

            check!(row.error_code == 0, "{label}");
            check!(batch.records.is_empty() == !altered, "{label}");
            check!(
                batch.altered
                    == if altered {
                        ["foo-0".to_owned()].into()
                    } else {
                        HashSet::new()
                    },
                "{label}"
            );
        }
        handle.shutdown().await;
    }

    #[test]
    fn a_topic_wide_proposal_is_spent_once_for_every_partition_it_covers() {
        let mut batch = ReassignBatch::default();
        let consumed =
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..approved_proposal(BreakGlassAction::CancelReassignment, "foo")
            });

        let first = batch.spend(Some(consumed.clone()));
        let second = batch.spend(Some(consumed.clone()));

        check!(first == Some(APPROVED_PROPOSAL_ID));
        check!(second == Some(APPROVED_PROPOSAL_ID));
        assert!(batch.records == vec![consumed]);
    }
    fn cancel(
        broker: &Broker,
        image: &MetadataImage,
    ) -> (ReassignablePartitionResponse, ReassignBatch) {
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("admin"),
            client_id = "reassign-client",
            address = crate::test_support::peer()
        );
        let env = ReassignEnv {
            broker,
            image,
            ctx: &ctx,
            allow_rf_change: true,
        };
        let mut batch = ReassignBatch::default();

        let row = alter_one(
            &env,
            &mut batch,
            "foo",
            &ReassignablePartition {
                partition_index: 0,
                replicas: None,
                ..Default::default()
            },
            FreezeMutationResolution::Admit,
        );
        (row, batch)
    }
}
