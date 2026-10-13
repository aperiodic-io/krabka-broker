//! `DescribeBreakGlass`, api key 1019.
//!
//! One request reads break-glass proposals. A nil `proposal_id` asks for every
//! proposal the controller holds, and `pending_only` drops the proposals that a
//! transition consumed, that an operator withdrew, or that expired.
//!
//! Each approval comes back with the `key_id` and the signature the approver
//! sent. The response gives them back on purpose, so an operator tool
//! re-verifies each approval against the operator public keys on its own
//! machine, and does not have to trust the broker that served the answer.
//!
//! Authorization: `Describe` on `Cluster("kafka-cluster")`. A denied request
//! answers `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! # A read writes no audit event, and a refused read does
//!
//! A describe changes nothing, so it fits none of the six privileged phases. A
//! refusal is different: a denied read of the break-glass surface is a signal
//! an auditor should see, so this handler audits one.

use {
    crate::{
        break_glass::{
            action_to_wire,
            config::BreakGlassPolicy,
            handlers::{
                PrivilegedAudit, Refusal, UNKNOWN_ACTION, audit_privileged,
                cluster_describe_denied, from_wire_uuid, to_wire_uuid,
            },
        },
        codes,
        handlers::encode_response,
    },
    krabka_audit::{AuditOutcome, PrivilegedPhase},
    krabka_metadata::{BreakGlassProposalRecord, MetadataImage},
    krabka_protocol::krabka::break_glass::{
        BreakGlassApproval as WireApproval, DescribeBreakGlassRequest, DescribeBreakGlassResponse,
        DescribedBreakGlassProposal,
    },
    uuid::Uuid,
};

wire_handler!(
    handle,
    "handle_describe_break_glass",
    "DescribeBreakGlass",
    "info",
    DescribeBreakGlassRequest,
    |broker, version, req, ctx| {
        let image = broker.controller.current_image();
        let outcome = if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            Err(Refusal::new(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                "describe-break-glass denied",
            ))
        } else {
            select(
                &image,
                req.pending_only,
                wanted_id(req.proposal_id),
                crate::time_util::now_ms(),
            )
        };

        let response = match outcome {
            Ok(proposals) => DescribeBreakGlassResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                error_message: None,
                proposals,
                ..DescribeBreakGlassResponse::default()
            },
            Err(refusal) => {
                let policy = BreakGlassPolicy::new(&broker.config.break_glass);
                audit_privileged(
                    broker.audit_log.as_ref(),
                    ctx,
                    policy.fingerprint(),
                    &PrivilegedAudit {
                        outcome: AuditOutcome::Failure,
                        phase: PrivilegedPhase::Refused,
                        action: UNKNOWN_ACTION,
                        target: "",
                        proposal_id: wanted_id(req.proposal_id),
                        counterparties: &[],
                        key_id: "",
                        signature: &[],
                        signature_verified: false,
                        reason: &refusal.message,
                    },
                );
                DescribeBreakGlassResponse {
                    throttle_time_ms: 0,
                    error_code: refusal.code,
                    error_message: Some(refusal.message),
                    ..DescribeBreakGlassResponse::default()
                }
            }
        };
        encode_response(&response, version)
    }
);

/// The proposal a request names, or `None` when it asks for every proposal.
fn wanted_id(id: krabka_protocol::primitives::uuid::Uuid) -> Option<Uuid> {
    let id = from_wire_uuid(id);
    (!id.is_nil()).then_some(id)
}

/// The proposals a request matches, in a stable order.
///
/// `wanted` names one proposal, and `None` asks for every one. `pending_only`
/// keeps the proposals that can still authorize a transition, so it drops the
/// consumed, the withdrawn, and the expired ones.
///
/// The answer is sorted by creation time, and then by proposal id, because
/// [`MetadataImage::break_glass_proposals`] defines no order. Two calls against
/// one image then give one answer.
///
/// # Errors
///
/// Returns [`Refusal`] with `RESOURCE_NOT_FOUND` (91) when `wanted` names a
/// proposal the image does not hold. A request that matches no proposal because
/// of `pending_only` is not an error, and answers an empty list.
pub(crate) fn select(
    image: &MetadataImage,
    pending_only: bool,
    wanted: Option<Uuid>,
    now_ms: i64,
) -> Result<Vec<DescribedBreakGlassProposal>, Refusal> {
    let mut matched: Vec<&BreakGlassProposalRecord> = match wanted {
        Some(id) => vec![image.break_glass_proposal(id).ok_or_else(|| {
            Refusal::new(
                codes::RESOURCE_NOT_FOUND,
                format!("no break-glass proposal {id}"),
            )
        })?],
        None => image.break_glass_proposals().collect(),
    };
    if pending_only {
        matched.retain(|proposal| is_pending(proposal, now_ms));
    }
    matched.sort_by(|left, right| {
        (left.created_at_ms, left.proposal_id).cmp(&(right.created_at_ms, right.proposal_id))
    });
    Ok(matched.into_iter().map(described).collect())
}

/// Whether a proposal can still authorize a transition.
fn is_pending(proposal: &BreakGlassProposalRecord, now_ms: i64) -> bool {
    !proposal.withdrawn && proposal.consumed_at_ms == 0 && now_ms < proposal.expires_at_ms
}

/// One proposal in the shape the wire carries.
fn described(proposal: &BreakGlassProposalRecord) -> DescribedBreakGlassProposal {
    DescribedBreakGlassProposal {
        proposal_id: to_wire_uuid(proposal.proposal_id),
        action: action_to_wire(proposal.action),
        target: proposal.target.clone(),
        proposer: proposal.proposer.clone(),
        reason: proposal.reason.clone(),
        created_at_ms: proposal.created_at_ms,
        expires_at_ms: proposal.expires_at_ms,
        consumed_at_ms: proposal.consumed_at_ms,
        withdrawn: proposal.withdrawn,
        approvals: proposal
            .approvals
            .iter()
            .map(|approval| WireApproval {
                principal: approval.principal.clone(),
                approved_at_ms: approval.approved_at_ms,
                key_id: approval.key_id.clone(),
                signature: approval.signature.clone(),
                ..WireApproval::default()
            })
            .collect(),
        ..DescribedBreakGlassProposal::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::break_glass::{
            ALL_ACTIONS, action_name,
            gate::tests::{CREATED_MS, EXPIRES_MS, NOW_MS, image_of, proposal, signed_approval},
        },
        assert2::{assert, check},
        krabka_metadata::BreakGlassAction,
    };

    fn ids(described: &[DescribedBreakGlassProposal]) -> Vec<Uuid> {
        described
            .iter()
            .map(|proposal| from_wire_uuid(proposal.proposal_id))
            .collect()
    }

    fn registry() -> MetadataImage {
        image_of(&[
            proposal(1, BreakGlassAction::DeleteTopic, "doomed"),
            BreakGlassProposalRecord {
                created_at_ms: CREATED_MS + 1,
                consumed_at_ms: NOW_MS - 1,
                ..proposal(2, BreakGlassAction::DeleteRecords, "orders-3")
            },
            BreakGlassProposalRecord {
                created_at_ms: CREATED_MS + 2,
                withdrawn: true,
                ..proposal(3, BreakGlassAction::UnregisterBroker, "7")
            },
            BreakGlassProposalRecord {
                created_at_ms: CREATED_MS + 3,
                expires_at_ms: NOW_MS,
                ..proposal(4, BreakGlassAction::ThawTopicFreeze, "literal:orders")
            },
        ])
    }

    #[test]
    fn a_read_of_the_whole_registry_answers_every_proposal_in_a_stable_order() {
        let described = select(&registry(), false, None, NOW_MS).expect("read the registry");

        check!(
            ids(&described)
                == [
                    Uuid::from_u128(1),
                    Uuid::from_u128(2),
                    Uuid::from_u128(3),
                    Uuid::from_u128(4),
                ]
        );
    }

    #[test]
    fn pending_only_drops_the_proposals_that_authorize_nothing() {
        let described = select(&registry(), true, None, NOW_MS).expect("read the registry");

        check!(ids(&described) == [Uuid::from_u128(1)]);
    }

    #[test]
    fn a_named_proposal_comes_back_alone() {
        let described = select(&registry(), false, Some(Uuid::from_u128(3)), NOW_MS)
            .expect("read one proposal");

        check!(ids(&described) == [Uuid::from_u128(3)]);
    }

    #[test]
    fn a_named_proposal_that_pending_only_drops_answers_an_empty_list() {
        let described =
            select(&registry(), true, Some(Uuid::from_u128(3)), NOW_MS).expect("read one proposal");

        check!(described.is_empty());
    }

    #[test]
    fn an_unknown_proposal_is_a_refusal_and_not_an_empty_list() {
        let outcome = select(&registry(), false, Some(Uuid::from_u128(99)), NOW_MS);

        assert!(let Err(refusal) = outcome);
        check!(refusal.code == codes::RESOURCE_NOT_FOUND);
        check!(refusal.message.contains(&Uuid::from_u128(99).to_string()));
    }

    #[test]
    fn an_empty_registry_answers_an_empty_list() {
        let image = image_of(&[]);

        let described = select(&image, false, None, NOW_MS).expect("read the empty registry");

        check!(described.is_empty());
    }

    #[test]
    fn a_described_proposal_carries_every_field_and_every_approval() {
        let stored = BreakGlassProposalRecord {
            approvals: vec![
                signed_approval("User:bob", "bob-yubi"),
                krabka_metadata::BreakGlassApproval {
                    key_id: String::new(),
                    signature: Vec::new(),
                    ..signed_approval("User:carol", "carol-yubi")
                },
            ],
            ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
        };
        let image = image_of(std::slice::from_ref(&stored));

        let described = select(&image, false, None, NOW_MS).expect("read the registry");

        let expected = DescribedBreakGlassProposal {
            proposal_id: to_wire_uuid(stored.proposal_id),
            action: 6,
            target: "doomed".to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: CREATED_MS,
            expires_at_ms: EXPIRES_MS,
            consumed_at_ms: 0,
            withdrawn: false,
            approvals: vec![
                WireApproval {
                    principal: "User:bob".to_owned(),
                    approved_at_ms: CREATED_MS + 1_000,
                    key_id: "bob-yubi".to_owned(),
                    signature: vec![7; 64],
                    ..WireApproval::default()
                },
                WireApproval {
                    principal: "User:carol".to_owned(),
                    approved_at_ms: CREATED_MS + 1_000,
                    key_id: String::new(),
                    signature: Vec::new(),
                    ..WireApproval::default()
                },
            ],
            ..DescribedBreakGlassProposal::default()
        };
        check!(described == vec![expected]);
    }

    #[test]
    fn every_action_reaches_the_wire_as_its_own_value() {
        for action in ALL_ACTIONS {
            let image = image_of(&[proposal(1, action, "target")]);

            let described = select(&image, false, None, NOW_MS).expect("read the registry");

            check!(
                described[0].action == action_to_wire(action),
                "{}",
                action_name(action)
            );
        }
    }

    #[test]
    fn a_nil_wire_id_asks_for_every_proposal() {
        let nil = krabka_protocol::primitives::uuid::Uuid::ZERO;

        check!(wanted_id(nil) == None);
        check!(wanted_id(to_wire_uuid(Uuid::from_u128(5))) == Some(Uuid::from_u128(5)));
    }

    #[test]
    fn a_proposal_that_expires_exactly_now_is_no_longer_pending() {
        let stored = proposal(1, BreakGlassAction::DeleteTopic, "doomed");

        check!(is_pending(&stored, EXPIRES_MS - 1));
        check!(!is_pending(&stored, EXPIRES_MS));
    }
}
